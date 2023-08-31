using NetTopologySuite.Geometries;
using NetTopologySuite.IO;
using Npgsql;
using OpenFTTH.AddressSearchIndexer;

internal sealed class PolygonDatabase
{
    private readonly DatabasePolygonSetting _settings;

    public PolygonDatabase(DatabasePolygonSetting databasePolygonSetting)
    {
        _settings = databasePolygonSetting;
    }

    public List<Polygon> RetrievePolygons()
    {
        // ST_DUMP is used to handle possible multi-polygons.
        var selectAllSupplyZonePolygonsQuery = @$"
SELECT ST_AsText((ST_DUMP({_settings.GeoemtryFieldName})).geom)
FROM {_settings.TableName}
";

        using var connection = new NpgsqlConnection(
            _settings.ConnectionString);
        using var command = new NpgsqlCommand(
            selectAllSupplyZonePolygonsQuery,
            connection);

        connection.Open();

        var reader = command.ExecuteReader();
        var wktReader = new WKTReader();

        var supplyZonePolygons = new List<Polygon>();
        while (reader.Read())
        {
            var geometry = wktReader.Read(reader.GetString(0));
            if (geometry is Polygon polygon)
            {
                supplyZonePolygons.Add(polygon);
            }
            else
            {
                throw new InvalidOperationException(
                    "The provided WKT does not represent a polygon.");
            }
        }

        return supplyZonePolygons;
    }
}
