using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpenFTTH.EventSourcing;
using System.Diagnostics;

namespace OpenFTTH.AddressSearchIndexer;

internal sealed class AddressSearchIndexerHost : BackgroundService
{
    private readonly ILogger<AddressSearchIndexerHost> _logger;
    private readonly Setting _setting;
    private readonly IEventStore _eventStore;
    private const int _catchUpTimeMs = 60000; // 1 min.

    public AddressSearchIndexerHost(
        ILogger<AddressSearchIndexerHost> logger,
        Setting setting,
        IEventStore eventStore)
    {
        _logger = logger;
        _setting = setting;
        _eventStore = eventStore;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation($"Starting {nameof(AddressSearchIndexerHost)}.");

        _logger.LogInformation("Starting dehydration.");
        await _eventStore.DehydrateProjectionsAsync(stoppingToken).ConfigureAwait(false);

        _logger.LogInformation(
            "Memory after dehydration {MibiBytes}.",
            Process.GetCurrentProcess().PrivateMemorySize64 / 1024 / 1024);

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(_catchUpTimeMs, stoppingToken).ConfigureAwait(false);
            _logger.LogInformation("Checking for new events.");
            await _eventStore.CatchUpAsync(stoppingToken).ConfigureAwait(false);
            _logger.LogInformation("Finished checking for new events.");
        }
    }
}
