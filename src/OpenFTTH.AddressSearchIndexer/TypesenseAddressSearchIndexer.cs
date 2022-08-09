using Microsoft.Extensions.Logging;
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

internal sealed class TypesenseAddressSearchIndexer : IAddressSearchIndexer
{
    private readonly ITypesenseClient _typesenseClient;
    private readonly ILogger<TypesenseAddressSearchIndexer> _logger;
    private readonly Setting _setting;

    public TypesenseAddressSearchIndexer(
        ITypesenseClient typesenseClient,
        ILogger<TypesenseAddressSearchIndexer> logger,
        Setting setting)
    {
        _typesenseClient = typesenseClient;
        _logger = logger;
        _setting = setting;
    }

    public async Task InitialCleanup()
    {
        var collectionAliasTask = RetrieveCollectionAlias();
        var collectionsTask = _typesenseClient.RetrieveCollections();

        var collectionAlias = await collectionAliasTask.ConfigureAwait(false);
        var collections = await collectionsTask.ConfigureAwait(false);

        // We want all the collections that follows the collection name scheme
        // that is not the current aliased collection.
        var collectionToBeDeleted = collections
            .Select(x => x.Name)
            .Where(x => x.StartsWith(_setting.Typesense.CollectionAlias,
                                     StringComparison.InvariantCulture))
            .Where(x => x != collectionAlias?.CollectionName);

        foreach (var c in collectionToBeDeleted)
        {
            _logger.LogInformation("Deleting dead collection '{Collection}'.", c);
            await _typesenseClient.DeleteCollection(c).ConfigureAwait(false);
        }
    }

    public async Task Index(AddressSearchIndexProjection projection)
    {
        var collectionName = $"{_setting.Typesense.CollectionAlias}-{Guid.NewGuid()}";

        _logger.LogInformation("Creating collection {CollectionName}.", collectionName);
        await CreateCollection(collectionName).ConfigureAwait(false);

        _logger.LogInformation("Starting indexing to Typesense.");
        var count = await IndexProjection(projection, collectionName)
            .ConfigureAwait(false);
        _logger.LogInformation(
            "Finished indexing a total of {Total} documents to Typesense.", count);

        var previousCollectionAlias =
            await RetrieveCollectionAlias().ConfigureAwait(false);

        _logger.LogInformation("Switching {Alias} to {CollectionAlias}.", _setting.Typesense.CollectionAlias, collectionName);
        await _typesenseClient
            .UpsertCollectionAlias(_setting.Typesense.CollectionAlias, new(collectionName))
            .ConfigureAwait(false);

        // We delete the old collection since it is not needed anymore.
        await DeletePreviousCollectionIfExists(previousCollectionAlias)
            .ConfigureAwait(false);
    }

    private async Task DeletePreviousCollectionIfExists(
        CollectionAliasResponse? previousCollectionAlias)
    {
        if (previousCollectionAlias is not null)
        {
            _logger.LogInformation(
                "Removing old {Collection}.",
                previousCollectionAlias.CollectionName);

            await _typesenseClient
                .DeleteCollection(previousCollectionAlias.CollectionName)
                .ConfigureAwait(false);
        }
    }

    private async Task<CollectionAliasResponse?> RetrieveCollectionAlias()
    {
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

        return previousCollectionAlias;
    }

    private async Task<int> IndexProjection(
        AddressSearchIndexProjection projection,
        string collectionName)
    {
        var imports = new List<TypesenseAddress>();
        var count = 0;
        foreach (var address in projection.IdToAddress.Values)
        {
            count++;
            if (imports.Count == _setting.Typesense.BatchSize)
            {
                _ = await _typesenseClient
                    .ImportDocuments(collectionName, imports, 250)
                    .ConfigureAwait(false);

                imports.Clear();
            }

            var roadName = projection.IdToRoadName[address.RoadId];
            var postCode = projection.IdToPostCode[address.PostCodeId];

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

        return count;
    }

    private async Task CreateCollection(string collectionName)
    {
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

        _ = await _typesenseClient.CreateCollection(schema).ConfigureAwait(false);
    }
}
