namespace OpenFTTH.AddressSearchIndexer;

internal interface IAddressSearchIndexer
{
    Task Index(AddressSearchIndexProjection projection);
}
