using Microsoft.Extensions.Logging;
using OpenFTTH.Core.Address.Events;
using OpenFTTH.EventSourcing;

namespace OpenFTTH.AddressSearchIndexer;

public sealed class AddressSearchIndexProjection : ProjectionBase
{
    private readonly ILogger<AddressSearchIndexProjection> _logger;
    private uint _count;

    public AddressSearchIndexProjection(ILogger<AddressSearchIndexProjection> logger)
    {
        _logger = logger;

        ProjectEventAsync<AccessAddressCreated>(ProjectAsync);
        ProjectEventAsync<AccessAddressUpdated>(ProjectAsync);
        ProjectEventAsync<AccessAddressDeleted>(ProjectAsync);
    }

    private async Task ProjectAsync(IEventEnvelope eventEnvelope)
    {
        _count++;

        switch (eventEnvelope.Data)
        {
            case (AccessAddressCreated accessAddressCreated):
                await Handle(accessAddressCreated).ConfigureAwait(false);
                break;
            case (AccessAddressUpdated accessAddressUpdated):
                await Handle(accessAddressUpdated).ConfigureAwait(false);
                break;
            case (AccessAddressDeleted accessAddressDeleted):
                await Handle(accessAddressDeleted).ConfigureAwait(false);
                break;
            default:
                throw new ArgumentException(
                    $"Could not handle typeof '{eventEnvelope.Data.GetType().Name}'");
        }

        if (_count % 10000 == 0)
        {
            _logger.LogInformation("{Count}", _count);
        }
    }

    private static async Task Handle(AccessAddressCreated accessAddressCreated)
    {
        await Task.CompletedTask.ConfigureAwait(false);
    }

    private static async Task Handle(AccessAddressUpdated accessAddressUpdated)
    {
        await Task.CompletedTask.ConfigureAwait(false);
    }

    private static async Task Handle(AccessAddressDeleted accessAddressDeleted)
    {
        await Task.CompletedTask.ConfigureAwait(false);
    }
}
