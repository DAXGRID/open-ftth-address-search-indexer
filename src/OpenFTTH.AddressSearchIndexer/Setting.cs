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

    [JsonConstructor]
    public TypesenseSetting(Uri uri, string key, string collectionAlias)
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

        Uri = uri;
        Key = key;
        CollectionAlias = collectionAlias;
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
