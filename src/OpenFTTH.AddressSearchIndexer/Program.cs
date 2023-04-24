using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpenFTTH.EventSourcing;

namespace OpenFTTH.AddressSearchIndexer;

internal static class Program
{
    public static async Task Main()
    {
        using var host = HostConfig.Configure();
            var logger = host.Services
            .GetService<ILoggerFactory>()
            !.CreateLogger(nameof(Program));

        try
        {
            if (logger is null)
            {
                throw new InvalidOperationException(
                    $"{nameof(ILogger)} is not configured.");
            }

            host.Services.GetService<IEventStore>()!.ScanForProjections();
            await host.StartAsync().ConfigureAwait(false);
            await host.WaitForShutdownAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger!.LogCritical("{Exception}", ex);
            throw;
        }
    }
}
