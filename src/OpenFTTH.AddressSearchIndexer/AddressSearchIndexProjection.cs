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

    public readonly Dictionary<Guid, PostCode> IdToPostCode = new();
    public readonly Dictionary<Guid, string> IdToRoadName = new();
    public readonly Dictionary<Guid, AccessAddress> IdToAddress = new();

    public AddressSearchIndexProjection(
        ILogger<AddressSearchIndexProjection> logger)
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
        IdToAddress.Add(
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
        var oldAccessAddress = IdToAddress[accessAddressRoadIdChanged.Id];
        IdToAddress[accessAddressRoadIdChanged.Id] = oldAccessAddress with
        {
            RoadId = accessAddressRoadIdChanged.RoadId,
        };
    }

    private void HandleAccessAddressSupplementaryTownNameChanged(AccessAddressSupplementaryTownNameChanged accessAddressSupplementaryTownNameChanged)
    {
        var oldAccessAddress = IdToAddress[accessAddressSupplementaryTownNameChanged.Id];
        IdToAddress[accessAddressSupplementaryTownNameChanged.Id] = oldAccessAddress with
        {
            TownName = accessAddressSupplementaryTownNameChanged.SupplementaryTownName,
        };
    }

    private void HandleAccessAddressPostCodeIdChanged(AccessAddressPostCodeIdChanged accessAddressPostCodeIdChanged)
    {
        var oldAccessAddress = IdToAddress[accessAddressPostCodeIdChanged.Id];
        IdToAddress[accessAddressPostCodeIdChanged.Id] = oldAccessAddress with
        {
            PostCodeId = accessAddressPostCodeIdChanged.PostCodeId,
        };
    }

    private void HandleAccessAddressHouseNumberChanged(AccessAddressHouseNumberChanged accessAddressHouseNumberChanged)
    {
        var oldAccessAddress = IdToAddress[accessAddressHouseNumberChanged.Id];
        IdToAddress[accessAddressHouseNumberChanged.Id] = oldAccessAddress with
        {
            HouseNumber = accessAddressHouseNumberChanged.HouseNumber
        };
    }

    private void HandleAccessAddressCoordinateChanged(AccessAddressCoordinateChanged accessAddressCoordinateChanged)
    {
        var oldAccessAddress = IdToAddress[accessAddressCoordinateChanged.Id];
        IdToAddress[accessAddressCoordinateChanged.Id] = oldAccessAddress with
        {
            NorthCoordinate = accessAddressCoordinateChanged.NorthCoordinate,
            EastCoordinate = accessAddressCoordinateChanged.EastCoordinate
        };
    }

    private void HandleAccessAddressDeleted(AccessAddressDeleted accessAddressDeleted)
    {
        IdToAddress.Remove(accessAddressDeleted.Id);
    }

    private void HandlePostCodeCreated(PostCodeCreated postCodeCreated)
    {
        IdToPostCode.Add(
            postCodeCreated.Id,
            new(postCodeCreated.Number, postCodeCreated.Name));
    }

    private void HandlePostCodeNameChanged(PostCodeNameChanged postCodeNameChanged)
    {
        var postCode = IdToPostCode[postCodeNameChanged.Id];
        IdToPostCode[postCodeNameChanged.Id] = postCode with
        {
            Name = postCodeNameChanged.Name
        };
    }

    private void HandlePostCodeDeleted(PostCodeDeleted postCodeDeleted)
    {
        IdToPostCode.Remove(postCodeDeleted.Id);
    }

    private void HandleRoadCreated(RoadCreated roadCreated)
    {
        IdToRoadName.Add(roadCreated.Id, roadCreated.Name);
    }

    private void HandleRoadNameChanged(RoadNameChanged roadNameChanged)
    {
        IdToRoadName[roadNameChanged.Id] = roadNameChanged.Name;
    }

    private void HandleRoadDeleted(RoadDeleted roadDeleted)
    {
        IdToRoadName.Remove(roadDeleted.Id);
    }

    public override Task DehydrationFinishAsync()
    {
        _logger.LogInformation("Finished dehydration a total of {Count} events.", _count);
        return Task.CompletedTask;
    }
}
