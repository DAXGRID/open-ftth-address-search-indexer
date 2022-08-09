using System.Text.Json.Serialization;

namespace OpenFTTH.AddressSearchIndexer;

internal sealed record TypesenseSetting
{
    [JsonPropertyName("uri")]
    public Uri Uri { get; init; }

    [JsonPropertyName("key")]
    public string Key { get; init; }

    [JsonPropertyName("collectionAlias")]
    public string CollectionAlias { get; init; }

    [JsonPropertyName("batchSize")]
    public uint BatchSize { get; init; }

    [JsonConstructor]
    public TypesenseSetting(Uri uri, string key, string collectionAlias, uint batchSize)
    {
        if (String.IsNullOrWhiteSpace(uri.AbsoluteUri))
        {
            throw new ArgumentException(
                "Cannot be null or whitespace.", nameof(uri));
        }

        if (String.IsNullOrWhiteSpace(key))
        {
            throw new ArgumentException(
                "Cannot be null or whitespace.", nameof(key));
        }

        if (String.IsNullOrWhiteSpace(collectionAlias))
        {
            throw new ArgumentException(
                "Cannot be null or whitespace.", nameof(collectionAlias));
        }

        if (batchSize == 0)
        {
            throw new ArgumentException("Must be greater than 0.", nameof(batchSize));
        }

        Uri = uri;
        Key = key;
        CollectionAlias = collectionAlias;
        BatchSize = batchSize;
    }
}

internal sealed record Setting
{
    [JsonPropertyName("eventStoreConnectionString")]
    public string EventStoreConnectionString { get; init; }
    [JsonPropertyName("typesense")]
    public TypesenseSetting Typesense { get; init; }

    [JsonConstructor]
    public Setting(string eventStoreConnectionString, TypesenseSetting typesense)
    {
        if (String.IsNullOrWhiteSpace(eventStoreConnectionString))
        {
            throw new ArgumentException(
                "Cannot be null or whitespace.", nameof(eventStoreConnectionString));
        }

        EventStoreConnectionString = eventStoreConnectionString;
        Typesense = typesense;
    }
}
