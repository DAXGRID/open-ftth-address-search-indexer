using Microsoft.Extensions.Logging;
using OpenFTTH.Core.Address.Events;
using OpenFTTH.EventSourcing;
using System.Text.Json.Serialization;

namespace OpenFTTH.AddressSearchIndexer;

internal sealed record TypesenseAddress
{
    [JsonPropertyName("id")]
    public string Id { get; init; }

    [JsonPropertyName("roadNameHouseNumber")]
    public string RoadNameHouseNumber { get; init; }

    [JsonPropertyName("townName")]
    public string TownName { get; init; }

    [JsonPropertyName("postDistrictCode")]
    public string PostDistrictCode { get; init; }

    [JsonPropertyName("postDistrictName")]
    public string PostDistrictName { get; init; }

    public TypesenseAddress(
        string id,
        string roadNameHouseNumber,
        string townName,
        string postDistrictCode,
        string postDistrictName)
    {
        Id = id;
        RoadNameHouseNumber = roadNameHouseNumber;
        TownName = townName;
        PostDistrictCode = postDistrictCode;
        PostDistrictName = postDistrictName;
    }
}

internal sealed class AddressSearchIndexProjection : ProjectionBase
{
    private readonly ILogger<AddressSearchIndexProjection> _logger;
    private uint _count;

    public AddressSearchIndexProjection(
        ILogger<AddressSearchIndexProjection> logger)
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
