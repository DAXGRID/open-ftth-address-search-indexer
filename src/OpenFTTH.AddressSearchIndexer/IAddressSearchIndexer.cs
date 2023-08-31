using NetTopologySuite.Geometries;

namespace OpenFTTH.AddressSearchIndexer;

internal interface IAddressSearchIndexer
{
    Task InitialCleanup();
    /// <summary>
    /// Indexes the projection into Typesense.
    /// The property `indexInsidePolygon` should always be supplied, but if it is empty everything will be indexed,
    /// otherwise only addresses inside of one or more of the supplied polygons will be indexed.
    /// </summary>
    Task Index(AddressSearchIndexProjection projection, IReadOnlyCollection<Polygon> indexInsidePolygon);
}
