using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace OpenFTTH.AddressSearchIndexer;

internal sealed class AddressSearchIndexerHost : BackgroundService
{
    private readonly ILogger<AddressSearchIndexerHost> _logger;
    private readonly Setting _setting;

    public AddressSearchIndexerHost(
        ILogger<AddressSearchIndexerHost> logger,
        Setting setting)
    {
        _logger = logger;
        _setting = setting;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation($"Starting {nameof(AddressSearchIndexerHost)}.");

        _logger.LogInformation("{Settings}", JsonSerializer.Serialize(_setting));

        await Task.CompletedTask.ConfigureAwait(false);
    }
}
