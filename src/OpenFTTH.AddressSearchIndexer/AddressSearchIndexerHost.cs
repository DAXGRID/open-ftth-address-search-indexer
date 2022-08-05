using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace OpenFTTH.AddressSearchIndexer;

public sealed class AddressSearchIndexerHost : BackgroundService
{
    private readonly ILogger<AddressSearchIndexerHost> _logger;

    public AddressSearchIndexerHost(ILogger<AddressSearchIndexerHost> logger)
    {
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation($"Starting {nameof(AddressSearchIndexerHost)}.");
        await Task.CompletedTask.ConfigureAwait(false);
    }
}
