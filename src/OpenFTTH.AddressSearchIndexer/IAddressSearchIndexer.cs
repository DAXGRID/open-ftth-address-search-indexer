namespace OpenFTTH.AddressSearchIndexer;

internal interface IAddressSearchIndexer
{
    Task InitialCleanup();
    Task Index(AddressSearchIndexProjection projection);
}
