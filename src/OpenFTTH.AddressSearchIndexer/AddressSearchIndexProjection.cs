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
    public string? TownName { get; init; }

    [JsonPropertyName("postDistrictCode")]
    public string PostDistrictCode { get; init; }

    [JsonPropertyName("postDistrictName")]
    public string PostDistrictName { get; init; }

    public TypesenseAddress(
        string id,
        string roadNameHouseNumber,
        string? townName,
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
    private record PostDistrict(string Code, string Name);

    private uint _count;
    private readonly ILogger<AddressSearchIndexProjection> _logger;

    private readonly Dictionary<Guid, PostDistrict> _postCodeIdToPostCode = new();
    private readonly Dictionary<Guid, string> _roadIdToName = new();
    private readonly Dictionary<Guid, TypesenseAddress> _accessAddressIdToAddress = new();

    public AddressSearchIndexProjection(
        ILogger<AddressSearchIndexProjection> logger)
    {
        _logger = logger;

        ProjectEventAsync<PostCodeCreated>(ProjectAsync);
        ProjectEventAsync<PostCodeUpdated>(ProjectAsync);
        ProjectEventAsync<PostCodeDeleted>(ProjectAsync);

        ProjectEventAsync<RoadCreated>(ProjectAsync);
        ProjectEventAsync<RoadUpdated>(ProjectAsync);
        ProjectEventAsync<RoadDeleted>(ProjectAsync);

        ProjectEventAsync<AccessAddressCreated>(ProjectAsync);
        ProjectEventAsync<AccessAddressUpdated>(ProjectAsync);
        ProjectEventAsync<AccessAddressDeleted>(ProjectAsync);
    }

    private async Task ProjectAsync(IEventEnvelope eventEnvelope)
    {
        _count++;

        switch (eventEnvelope.Data)
        {
            case (PostCodeCreated postCodeCreated):
                HandlePostCodeCreated(postCodeCreated);
                break;
            case (PostCodeUpdated postCodeUpdated):
                HandlePostCodeUpdated(postCodeUpdated);
                break;
            case (PostCodeDeleted postCodeDeleted):
                HandlePostCodeDeleted(postCodeDeleted);
                break;
            case (RoadCreated roadCreated):
                HandleRoadCreated(roadCreated);
                break;
            case (RoadUpdated roadUpdated):
                HandleRoadUpdated(roadUpdated);
                break;
            case (RoadDeleted roadDeleted):
                HandleRoadDeleted(roadDeleted);
                break;
            case (AccessAddressCreated accessAddressCreated):
                HandleAccessAddressCreated(accessAddressCreated);
                break;
            case (AccessAddressUpdated accessAddressUpdated):
                HandleAccessAddressUpdated(accessAddressUpdated);
                break;
            case (AccessAddressDeleted accessAddressDeleted):
                HandleAccessAddressDeleted(accessAddressDeleted);
                break;
            default:
                throw new ArgumentException(
                    $"Could not handle typeof '{eventEnvelope.Data.GetType().Name}'");
        }

        if (_count % 10000 == 0)
        {
            _logger.LogInformation("{Count}", _count);
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }

    private void HandleAccessAddressCreated(AccessAddressCreated accessAddressCreated)
    {
        var roadName = _roadIdToName[accessAddressCreated.RoadId];
        var postDistrict = _postCodeIdToPostCode[accessAddressCreated.PostCodeId];

        _accessAddressIdToAddress.Add(
            accessAddressCreated.Id,
            new(
                id: accessAddressCreated.Id.ToString(),
                roadNameHouseNumber: $"{roadName} {accessAddressCreated.HouseNumber}",
                townName: accessAddressCreated.TownName,
                postDistrictCode: postDistrict.Code,
                postDistrictName: postDistrict.Name
            ));
    }

    private void HandleAccessAddressUpdated(AccessAddressUpdated accessAddressUpdated)
    {
        var oldAccessAddress = _accessAddressIdToAddress[accessAddressUpdated.Id];
        var postDistrict = _postCodeIdToPostCode[accessAddressUpdated.PostCodeId];
        var roadName = _roadIdToName[accessAddressUpdated.RoadId];

        _accessAddressIdToAddress[accessAddressUpdated.Id] = oldAccessAddress with
        {
            PostDistrictCode = postDistrict.Code,
            PostDistrictName = postDistrict.Name,
            RoadNameHouseNumber = $"{roadName} {accessAddressUpdated.HouseNumber}",
            TownName = accessAddressUpdated.TownName,
        };
    }

    private void HandleAccessAddressDeleted(AccessAddressDeleted accessAddressDeleted)
    {
        _accessAddressIdToAddress.Remove(accessAddressDeleted.Id);
    }

    private void HandlePostCodeCreated(PostCodeCreated postCodeCreated)
    {
        _postCodeIdToPostCode.Add(
            postCodeCreated.Id,
            new(postCodeCreated.Number, postCodeCreated.Name));
    }

    private void HandlePostCodeUpdated(PostCodeUpdated postCodeUpdated)
    {
        var postCode = _postCodeIdToPostCode[postCodeUpdated.Id];
        _postCodeIdToPostCode[postCodeUpdated.Id] = postCode with
        {
            Name = postCodeUpdated.Name
        };
    }

    private void HandlePostCodeDeleted(PostCodeDeleted postCodeDeleted)
    {
        _postCodeIdToPostCode.Remove(postCodeDeleted.Id);
    }

    private void HandleRoadCreated(RoadCreated roadCreated)
    {
        _roadIdToName.Add(roadCreated.Id, roadCreated.Name);
    }

    private void HandleRoadUpdated(RoadUpdated roadUpdated)
    {
        _roadIdToName[roadUpdated.Id] = roadUpdated.Name;
    }

    private void HandleRoadDeleted(RoadDeleted roadDeleted)
    {
        _roadIdToName.Remove(roadDeleted.Id);
    }
}
