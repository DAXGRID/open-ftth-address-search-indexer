using System.Text.Json.Serialization;

namespace OpenFTTH.AddressSearchIndexer;

internal sealed record TypesenseSetting
{
    [JsonPropertyName("uri")]
    public Uri Uri { get; init; }
    [JsonPropertyName("key")]
    public string Key { get; init; }
    [JsonPropertyName("collectionAliasName")]
    public string CollectionAliasName { get; init; }

    [JsonConstructor]
    public TypesenseSetting(Uri uri, string key, string collectionAliasName)
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

        if (String.IsNullOrWhiteSpace(collectionAliasName))
        {
            throw new ArgumentException(
                "Cannot be null or whitespace.", nameof(collectionAliasName));
        }

        Uri = uri;
        Key = key;
        CollectionAliasName = collectionAliasName;
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
