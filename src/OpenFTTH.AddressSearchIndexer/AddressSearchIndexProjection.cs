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
        ProjectEventAsync<PostCodeUpdated>(ProjectAsync);
        ProjectEventAsync<PostCodeDeleted>(ProjectAsync);

        ProjectEventAsync<RoadCreated>(ProjectAsync);
        ProjectEventAsync<RoadUpdated>(ProjectAsync);
        ProjectEventAsync<RoadDeleted>(ProjectAsync);

        ProjectEventAsync<AccessAddressCreated>(ProjectAsync);
        ProjectEventAsync<AccessAddressUpdated>(ProjectAsync);
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

    private void HandleAccessAddressUpdated(AccessAddressUpdated accessAddressUpdated)
    {
        var oldAccessAddress = IdToAddress[accessAddressUpdated.Id];
        IdToAddress[accessAddressUpdated.Id] = oldAccessAddress with
        {
            RoadId = accessAddressUpdated.RoadId,
            TownName = accessAddressUpdated.TownName,
            PostCodeId = accessAddressUpdated.PostCodeId,
            NorthCoordinate = accessAddressUpdated.NorthCoordinate,
            EastCoordinate = accessAddressUpdated.EastCoordinate
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

    private void HandlePostCodeUpdated(PostCodeUpdated postCodeUpdated)
    {
        var postCode = IdToPostCode[postCodeUpdated.Id];
        IdToPostCode[postCodeUpdated.Id] = postCode with
        {
            Name = postCodeUpdated.Name
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

    private void HandleRoadUpdated(RoadUpdated roadUpdated)
    {
        IdToRoadName[roadUpdated.Id] = roadUpdated.Name;
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
