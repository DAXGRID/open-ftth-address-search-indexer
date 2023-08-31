using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NetTopologySuite.Geometries;
using OpenFTTH.EventSourcing;
using System.Diagnostics;

namespace OpenFTTH.AddressSearchIndexer;

internal sealed class AddressSearchIndexerHost : BackgroundService
{
    private readonly ILogger<AddressSearchIndexerHost> _logger;
    private readonly IEventStore _eventStore;
    private readonly IAddressSearchIndexer _addressSearchIndexer;
    private readonly Setting _setting;
    private const int _catchUpTimeMs = 60000; // 1 min.

    public AddressSearchIndexerHost(
        ILogger<AddressSearchIndexerHost> logger,
        IEventStore eventStore,
        Setting setting,
        IAddressSearchIndexer addressSearchIndexer)
    {
        _logger = logger;
        _eventStore = eventStore;
        _setting = setting;
        _addressSearchIndexer = addressSearchIndexer;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation($"Starting {nameof(AddressSearchIndexerHost)}.");

        _logger.LogInformation("Starting initial cleanup.");
        await _addressSearchIndexer.InitialCleanup().ConfigureAwait(false);

        _logger.LogInformation("Starting dehydration.");
        await _eventStore
            .DehydrateProjectionsAsync(stoppingToken)
            .ConfigureAwait(false);

        _logger.LogInformation(
            "Memory after dehydration {MibiBytes}.",
            Process.GetCurrentProcess().PrivateMemorySize64 / 1024 / 1024);

        var polygonDatabase = _setting.DatabasePolygon is not null
            ? new PolygonDatabase(_setting.DatabasePolygon)
            : null;

        var indexInsidePolygons = polygonDatabase is not null
            ? polygonDatabase.RetrievePolygons().AsReadOnly()
            : new List<Polygon>().AsReadOnly();

        var projection = _eventStore.Projections.Get<AddressSearchIndexProjection>();

        await _addressSearchIndexer
            .Index(projection, indexInsidePolygons)
            .ConfigureAwait(false);

        _logger.LogInformation(
            "Memory after address indexing {MibiBytes}.",
            Process.GetCurrentProcess().PrivateMemorySize64 / 1024 / 1024);

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(_catchUpTimeMs, stoppingToken).ConfigureAwait(false);

            _logger.LogDebug("Checking for new events.");
            var changes = await _eventStore
                .CatchUpAsync(stoppingToken)
                .ConfigureAwait(false);

            var newIndexInsidePolygon = polygonDatabase is not null
                ? polygonDatabase.RetrievePolygons().AsReadOnly()
                : new List<Polygon>().AsReadOnly();

            var hasIndexInsidePolygonChanged =
                !(new GeometryCollection(indexInsidePolygons.ToArray())
                  .EqualsExact(new GeometryCollection(newIndexInsidePolygon.ToArray())));

            // Update to use the new index inside polygon for future comparisons.
            indexInsidePolygons = newIndexInsidePolygon;

            if (changes > 0)
            {
                _logger.LogInformation("{Count} changes so we do import.", changes);
                await _addressSearchIndexer
                    .Index(projection, indexInsidePolygons)
                    .ConfigureAwait(false);
            }
            else if (hasIndexInsidePolygonChanged)
            {
                _logger.LogInformation(
                    "The index inside polygons has changed, starting indexing projection.",
                    changes);

                await _addressSearchIndexer
                    .Index(projection, indexInsidePolygons)
                    .ConfigureAwait(false);
            }
            else
            {
                _logger.LogDebug("No changes since last run.");
            }
        }
    }
}
