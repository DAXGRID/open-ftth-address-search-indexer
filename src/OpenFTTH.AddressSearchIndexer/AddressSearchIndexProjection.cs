using Microsoft.Extensions.Logging;
using OpenFTTH.Core.Address.Events;
using OpenFTTH.EventSourcing;

namespace OpenFTTH.AddressSearchIndexer;

internal sealed record PostCode(string Code, string Name);

internal sealed record AccessAddress(
    Guid Id,
    Guid RoadId,
    string? TownName,
    string HouseNumber,
    Guid PostCodeId,
    double NorthCoordinate,
    double EastCoordinate);

internal sealed class AddressSearchIndexProjection : ProjectionBase
{
    private uint _count;
    private readonly ILogger<AddressSearchIndexProjection> _logger;
    private readonly Dictionary<Guid, PostCode> _idToPostCode = new();
    private readonly Dictionary<Guid, string> _idToRoadName = new();
    private readonly Dictionary<Guid, AccessAddress> _idToAddress = new();

    public IReadOnlyDictionary<Guid, PostCode> IdToPostCode => _idToPostCode;
    public IReadOnlyDictionary<Guid, string> IdToRoadName => _idToRoadName;
    public IReadOnlyDictionary<Guid, AccessAddress> IdToAddress => _idToAddress;

    public AddressSearchIndexProjection(ILogger<AddressSearchIndexProjection> logger)
    {
        _logger = logger;

        ProjectEventAsync<PostCodeCreated>(ProjectAsync);
        ProjectEventAsync<PostCodeNameChanged>(ProjectAsync);
        ProjectEventAsync<PostCodeDeleted>(ProjectAsync);

        ProjectEventAsync<RoadCreated>(ProjectAsync);
        ProjectEventAsync<RoadNameChanged>(ProjectAsync);
        ProjectEventAsync<RoadDeleted>(ProjectAsync);

        ProjectEventAsync<AccessAddressCreated>(ProjectAsync);
        ProjectEventAsync<AccessAddressRoadIdChanged>(ProjectAsync);
        ProjectEventAsync<AccessAddressSupplementaryTownNameChanged>(ProjectAsync);
        ProjectEventAsync<AccessAddressPostCodeIdChanged>(ProjectAsync);
        ProjectEventAsync<AccessAddressHouseNumberChanged>(ProjectAsync);
        ProjectEventAsync<AccessAddressCoordinateChanged>(ProjectAsync);
        ProjectEventAsync<AccessAddressDeleted>(ProjectAsync);
    }

    private Task ProjectAsync(IEventEnvelope eventEnvelope)
    {
        _count++;

        switch (eventEnvelope.Data)
        {
            case (PostCodeCreated postCodeCreated):
                HandlePostCodeCreated(postCodeCreated);
                break;
            case (PostCodeNameChanged postCodeNameChanged):
                HandlePostCodeNameChanged(postCodeNameChanged);
                break;
            case (PostCodeDeleted postCodeDeleted):
                HandlePostCodeDeleted(postCodeDeleted);
                break;
            case (RoadCreated roadCreated):
                HandleRoadCreated(roadCreated);
                break;
            case (RoadNameChanged roadNameChanged):
                HandleRoadNameChanged(roadNameChanged);
                break;
            case (RoadDeleted roadDeleted):
                HandleRoadDeleted(roadDeleted);
                break;
            case (AccessAddressCreated accessAddressCreated):
                HandleAccessAddressCreated(accessAddressCreated);
                break;
            case (AccessAddressRoadIdChanged accessAddressRoadIdChanged):
                HandleAccessAddressRoadIdChanged(accessAddressRoadIdChanged);
                break;
            case (AccessAddressSupplementaryTownNameChanged accessAddressSupplementaryTownNameChanged):
                HandleAccessAddressSupplementaryTownNameChanged(accessAddressSupplementaryTownNameChanged);
                break;
            case (AccessAddressPostCodeIdChanged accessAddressPostCodeIdChanged):
                HandleAccessAddressPostCodeIdChanged(accessAddressPostCodeIdChanged);
                break;
            case (AccessAddressHouseNumberChanged accessAddressHouseNumberChanged):
                HandleAccessAddressHouseNumberChanged(accessAddressHouseNumberChanged);
                break;
            case (AccessAddressCoordinateChanged accessAddressCoordinateChanged):
                HandleAccessAddressCoordinateChanged(accessAddressCoordinateChanged);
                break;
            case (AccessAddressDeleted accessAddressDeleted):
                HandleAccessAddressDeleted(accessAddressDeleted);
                break;
            default:
                throw new ArgumentException(
                    $"Could not handle typeof '{eventEnvelope.Data.GetType().Name}'");
        }

        return Task.CompletedTask;
    }

    private void HandleAccessAddressCreated(AccessAddressCreated accessAddressCreated)
    {
        _idToAddress.Add(
            accessAddressCreated.Id,
            new(
                Id: accessAddressCreated.Id,
                RoadId: accessAddressCreated.RoadId,
                HouseNumber: accessAddressCreated.HouseNumber,
                TownName: accessAddressCreated.TownName,
                PostCodeId: accessAddressCreated.PostCodeId,
                NorthCoordinate: accessAddressCreated.NorthCoordinate,
                EastCoordinate: accessAddressCreated.EastCoordinate
            ));
    }

    private void HandleAccessAddressRoadIdChanged(AccessAddressRoadIdChanged accessAddressRoadIdChanged)
    {
        var oldAccessAddress = _idToAddress[accessAddressRoadIdChanged.Id];
        _idToAddress[accessAddressRoadIdChanged.Id] = oldAccessAddress with
        {
            RoadId = accessAddressRoadIdChanged.RoadId,
        };
    }

    private void HandleAccessAddressSupplementaryTownNameChanged(AccessAddressSupplementaryTownNameChanged accessAddressSupplementaryTownNameChanged)
    {
        var oldAccessAddress = _idToAddress[accessAddressSupplementaryTownNameChanged.Id];
        _idToAddress[accessAddressSupplementaryTownNameChanged.Id] = oldAccessAddress with
        {
            TownName = accessAddressSupplementaryTownNameChanged.SupplementaryTownName,
        };
    }

    private void HandleAccessAddressPostCodeIdChanged(AccessAddressPostCodeIdChanged accessAddressPostCodeIdChanged)
    {
        var oldAccessAddress = _idToAddress[accessAddressPostCodeIdChanged.Id];
        _idToAddress[accessAddressPostCodeIdChanged.Id] = oldAccessAddress with
        {
            PostCodeId = accessAddressPostCodeIdChanged.PostCodeId,
        };
    }

    private void HandleAccessAddressHouseNumberChanged(AccessAddressHouseNumberChanged accessAddressHouseNumberChanged)
    {
        var oldAccessAddress = _idToAddress[accessAddressHouseNumberChanged.Id];
        _idToAddress[accessAddressHouseNumberChanged.Id] = oldAccessAddress with
        {
            HouseNumber = accessAddressHouseNumberChanged.HouseNumber
        };
    }

    private void HandleAccessAddressCoordinateChanged(AccessAddressCoordinateChanged accessAddressCoordinateChanged)
    {
        var oldAccessAddress = _idToAddress[accessAddressCoordinateChanged.Id];
        _idToAddress[accessAddressCoordinateChanged.Id] = oldAccessAddress with
        {
            NorthCoordinate = accessAddressCoordinateChanged.NorthCoordinate,
            EastCoordinate = accessAddressCoordinateChanged.EastCoordinate
        };
    }

    private void HandleAccessAddressDeleted(AccessAddressDeleted accessAddressDeleted)
    {
        _idToAddress.Remove(accessAddressDeleted.Id);
    }

    private void HandlePostCodeCreated(PostCodeCreated postCodeCreated)
    {
        _idToPostCode.Add(
            postCodeCreated.Id,
            new(postCodeCreated.Number, postCodeCreated.Name));
    }

    private void HandlePostCodeNameChanged(PostCodeNameChanged postCodeNameChanged)
    {
        var postCode = _idToPostCode[postCodeNameChanged.Id];
        _idToPostCode[postCodeNameChanged.Id] = postCode with
        {
            Name = postCodeNameChanged.Name
        };
    }

    private void HandlePostCodeDeleted(PostCodeDeleted postCodeDeleted)
    {
        _idToPostCode.Remove(postCodeDeleted.Id);
    }

    private void HandleRoadCreated(RoadCreated roadCreated)
    {
        _idToRoadName.Add(roadCreated.Id, roadCreated.Name);
    }

    private void HandleRoadNameChanged(RoadNameChanged roadNameChanged)
    {
        _idToRoadName[roadNameChanged.Id] = roadNameChanged.Name;
    }

    private void HandleRoadDeleted(RoadDeleted roadDeleted)
    {
        _idToRoadName.Remove(roadDeleted.Id);
    }

    public override Task DehydrationFinishAsync()
    {
        _logger.LogInformation("Finished dehydration a total of {Count} events.", _count);
        return Task.CompletedTask;
    }
}
