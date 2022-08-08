using Microsoft.Extensions.Logging;
using OpenFTTH.Core.Address.Events;
using OpenFTTH.EventSourcing;
using System.Diagnostics;
using System.Text.Json.Serialization;
using Typesense;

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
    public string PostCode { get; init; }

    [JsonPropertyName("postDistrictName")]
    public string PostCodeName { get; init; }

    [JsonPropertyName("northCoordinate")]
    public double NorthCoordinate { get; init; }

    [JsonPropertyName("eastCoordinate")]
    public double EastCoordinate { get; init; }

    public TypesenseAddress(
        string id,
        string roadNameHouseNumber,
        string? townName,
        string postCode,
        string postCodeName,
        double northCoordinate,
        double eastCoordinate)
    {
        Id = id;
        RoadNameHouseNumber = roadNameHouseNumber;
        TownName = townName;
        PostCode = postCode;
        PostCodeName = postCodeName;
        NorthCoordinate = northCoordinate;
        EastCoordinate = eastCoordinate;
    }
}

internal sealed class AddressSearchIndexProjection : ProjectionBase
{
    private record PostCode(string Code, string Name);
    private record AccessAddress(
        Guid Id,
        Guid RoadId,
        string? TownName,
        string HouseNumber,
        Guid PostCodeId,
        double NorthCoordinate,
        double EastCoordinate);

    private uint _count;
    private uint _bulkSize = 1000;
    private readonly ILogger<AddressSearchIndexProjection> _logger;
    private readonly ITypesenseClient _typesenseClient;
    private readonly Setting _setting;

    private readonly Dictionary<Guid, PostCode> _idToPostCode = new();
    private readonly Dictionary<Guid, string> _idToRoadName = new();
    private readonly Dictionary<Guid, AccessAddress> _idToAddress = new();

    public AddressSearchIndexProjection(
        ILogger<AddressSearchIndexProjection> logger,
        ITypesenseClient typesenseClient,
        Setting setting)
    {
        _logger = logger;
        _typesenseClient = typesenseClient;
        _setting = setting;

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

    private void HandleAccessAddressUpdated(AccessAddressUpdated accessAddressUpdated)
    {
        var oldAccessAddress = _idToAddress[accessAddressUpdated.Id];
        _idToAddress[accessAddressUpdated.Id] = oldAccessAddress with
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
        _idToAddress.Remove(accessAddressDeleted.Id);
    }

    private void HandlePostCodeCreated(PostCodeCreated postCodeCreated)
    {
        _idToPostCode.Add(
            postCodeCreated.Id,
            new(postCodeCreated.Number, postCodeCreated.Name));
    }

    private void HandlePostCodeUpdated(PostCodeUpdated postCodeUpdated)
    {
        var postCode = _idToPostCode[postCodeUpdated.Id];
        _idToPostCode[postCodeUpdated.Id] = postCode with
        {
            Name = postCodeUpdated.Name
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

    private void HandleRoadUpdated(RoadUpdated roadUpdated)
    {
        _idToRoadName[roadUpdated.Id] = roadUpdated.Name;
    }

    private void HandleRoadDeleted(RoadDeleted roadDeleted)
    {
        _idToRoadName.Remove(roadDeleted.Id);
    }

    public override async Task DehydrationFinishAsync()
    {
        _logger.LogInformation(
            "Finished dehydration with a total of {Count}.", _count);

        var collectionName = $"{_setting.Typesense.CollectionAlias}-{Guid.NewGuid()}";
        var schema = new Schema(
            collectionName,
            new List<Field>
            {
                new Field("id", FieldType.String, false),
                new Field("roadNameHouseNumber", FieldType.String, false),
                new Field("townName", FieldType.String, false, true),
                new Field("postDistrictCode", FieldType.String, false, false),
                new Field("postDistrictName", FieldType.String, false, false),
                new Field("eastCoordinate", FieldType.String, false, true, false),
                new Field("northCoordinate", FieldType.String, false, true, false),
            });

        _logger.LogInformation(
            "Creation collection {CollectionName}.", collectionName);
        _ = await _typesenseClient.CreateCollection(schema).ConfigureAwait(false);

        _logger.LogInformation(
            "Starting indexing to Typesense.");

        var timer = new Stopwatch();
        timer.Start();
        var imports = new List<TypesenseAddress>();
        var count = 0;
        foreach (var address in _idToAddress.Values)
        {
            count++;
            if (imports.Count == _bulkSize)
            {
                _ = await _typesenseClient
                    .ImportDocuments(collectionName, imports, 250)
                    .ConfigureAwait(false);

                imports.Clear();
            }

            var roadName = _idToRoadName[address.RoadId];
            var postCode = _idToPostCode[address.PostCodeId];

            var document = new TypesenseAddress(
                id: address.Id.ToString(),
                roadNameHouseNumber: $"{roadName} {address.HouseNumber}",
                townName: address.TownName,
                postCode: postCode.Code,
                postCodeName: postCode.Name,
                northCoordinate: address.NorthCoordinate,
                eastCoordinate: address.EastCoordinate);

            imports.Add(document);
        }

        // Import the remaining
        _ = await _typesenseClient
            .ImportDocuments(collectionName, imports)
            .ConfigureAwait(false);

        timer.Stop();

        _logger.LogInformation(
            @"Finished indexing a total of {Total} documents to Typesense.
 It took a total of {TotalTimeSec}.",
            count, timer.ElapsedMilliseconds / 1000);

        CollectionAliasResponse? previousCollectionAlias = null;
        try
        {
            previousCollectionAlias = await _typesenseClient
               .RetrieveCollectionAlias(_setting.Typesense.CollectionAlias)
               .ConfigureAwait(false);
        }
        catch (TypesenseApiNotFoundException)
        {
            // Do nothing this is valid in case when this is the first run,
            // because there won't be any collection alias created.
        }

        _logger.LogInformation("Updating alias to {CollectionAlias}.", collectionName);
        await _typesenseClient
            .UpsertCollectionAlias(_setting.Typesense.CollectionAlias, new(collectionName))
            .ConfigureAwait(false);

        // We delete the old collection since it is not needed anymore.
        if (previousCollectionAlias is not null)
        {

            _logger.LogInformation(
                "Removing old {Collection}.", previousCollectionAlias.CollectionName);
            await _typesenseClient
                .DeleteCollection(previousCollectionAlias.CollectionName)
                .ConfigureAwait(false);
        }
    }
}
