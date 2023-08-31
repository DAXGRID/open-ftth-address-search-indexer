using Microsoft.Extensions.Logging;
using NetTopologySuite.Geometries;
using System.Globalization;
using System.Text.Json;
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
    public string NorthCoordinate { get; init; }

    [JsonPropertyName("eastCoordinate")]
    public string EastCoordinate { get; init; }

    public TypesenseAddress(
        string id,
        string roadNameHouseNumber,
        string? townName,
        string postCode,
        string postCodeName,
        string northCoordinate,
        string eastCoordinate)
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
        var collectionsToBeDeleted = collections
            .Select(x => x.Name)
            .Where(x => x.StartsWith(_setting.Typesense.CollectionAlias,
                                     StringComparison.InvariantCulture))
            .Where(x => x != collectionAlias?.CollectionName)
            .ToList();

        foreach (var collectionToBeDeleted in collectionsToBeDeleted)
        {
            _logger.LogInformation(
                "Deleting dead collection '{Collection}'.",
                collectionToBeDeleted);

            await _typesenseClient
                .DeleteCollection(collectionToBeDeleted)
                .ConfigureAwait(false);
        }
    }

    public async Task Index(AddressSearchIndexProjection projection, IReadOnlyCollection<Polygon> indexInsidePolygons)
    {
        var collectionName = $"{_setting.Typesense.CollectionAlias}-{Guid.NewGuid()}";

        _logger.LogInformation("Creating collection {CollectionName}.", collectionName);
        await CreateCollection(collectionName).ConfigureAwait(false);

        _logger.LogInformation("Starting indexing to Typesense.");
        var count = await IndexProjection(projection, collectionName, indexInsidePolygons).ConfigureAwait(false);

        _logger.LogInformation(
            "Finished indexing a total of {Total} documents to Typesense.",
            count);

        var previousCollectionAlias = await RetrieveCollectionAlias().ConfigureAwait(false);

        _logger.LogInformation(
            "Switching {Alias} to {CollectionAlias}.",
            _setting.Typesense.CollectionAlias, collectionName);

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
        string collectionName,
        IReadOnlyCollection<Polygon> indexInsidePolygons)
    {
        var imports = new List<TypesenseAddress>();
        var count = 0;
        foreach (var address in projection.IdToAddress.Values)
        {
            // If no polygons are supplied we just index everything.
            if (indexInsidePolygons.Count > 0)
            {
                if (!indexInsidePolygons.Any(polygon => polygon.Intersects(new Point(address.NorthCoordinate, address.EastCoordinate))))
                {
                    // We only want to index if the addres is inside one or more of the supplied polygons.
                    continue;
                }
            }

            count++;
            if (imports.Count == _setting.Typesense.BatchSize)
            {
                _logger.LogInformation("Importing {AddressCount}.", imports.Count);
                var results = await _typesenseClient
                    .ImportDocuments(collectionName,
                                     imports,
                                     (int)_setting.Typesense.BatchSize)
                    .ConfigureAwait(false);

                if (results.Any(x => !x.Success))
                {
                    var failedDocuments = results.Where(x => !x.Success);

                    throw new InvalidOperationException(
                        @$"Not all imports were successfull.
{JsonSerializer.Serialize(failedDocuments)}");
                }

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
                northCoordinate: address.NorthCoordinate.ToString(
                    "G", CultureInfo.InvariantCulture),
                eastCoordinate: address.EastCoordinate.ToString(
                    "G", CultureInfo.InvariantCulture));

            imports.Add(document);
        }

        // Import the remaining
        _logger.LogInformation("Importing remaining {AddressCount}.", imports.Count);
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
