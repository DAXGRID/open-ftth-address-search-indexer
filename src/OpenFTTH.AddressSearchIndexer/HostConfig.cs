using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using OpenFTTH.EventSourcing;
using OpenFTTH.EventSourcing.Postgres;
using Serilog;
using Serilog.Events;
using Serilog.Formatting.Compact;
using System.Globalization;
using System.Reflection;
using System.Text.Json;
using Typesense.Setup;

namespace OpenFTTH.AddressSearchIndexer;

internal static class HostConfig
{
    public static IHost Configure()
    {
        var hostBuilder = new HostBuilder();
        ConfigureLogging(hostBuilder);
        ConfigureServices(hostBuilder);
        return hostBuilder.Build();
    }

    private static void ConfigureServices(HostBuilder hostBuilder)
    {
        var settingsJson = JsonDocument.Parse(File.ReadAllText("appsettings.json"))
            .RootElement.GetProperty("settings").ToString();

        var setting = JsonSerializer.Deserialize<Setting>(settingsJson) ??
            throw new ArgumentException(
                "Could not deserialize appsettings into settings.");

        hostBuilder.ConfigureServices((hostContext, services) =>
        {
            services.AddHostedService<AddressSearchIndexerHost>();
            services.AddSingleton<Setting>(setting);
            services.AddTypesenseClient(config =>
            {
                config.ApiKey = setting.Typesense.Key;
                config.Nodes = new List<Node>
                {
                    new Node(
                        host: setting.Typesense.Uri.Host,
                        port: setting.Typesense.Uri.Port.ToString(
                            "G", CultureInfo.InvariantCulture),
                        protocol: setting.Typesense.Uri.Scheme)
                };
            });
            services.AddSingleton<IEventStore>(
                e =>
                new PostgresEventStore(
                    serviceProvider: e.GetRequiredService<IServiceProvider>(),
                    connectionString: setting.EventStoreConnectionString,
                    databaseSchemaName: "events"));
            services.AddProjections(new Assembly[]
            {
                AppDomain.CurrentDomain.Load("OpenFTTH.AddressSearchIndexer")
            });
            services.AddSingleton<IAddressSearchIndexer, TypesenseAddressSearchIndexer>();
        });
    }

    private static void ConfigureLogging(HostBuilder hostBuilder)
    {
        hostBuilder.ConfigureServices((hostContext, services) =>
        {
            services.AddLogging(loggingBuilder =>
            {
                var logger = new LoggerConfiguration()
                    .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
                    .MinimumLevel.Override("System", LogEventLevel.Warning)
                    .Enrich.FromLogContext()
                    .WriteTo.Console(new CompactJsonFormatter())
                    .CreateLogger();

                loggingBuilder.AddSerilog(logger, true);
            });
        });
    }
}
