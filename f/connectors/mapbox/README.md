# `mapbox_replace_tileset_source`: Replace a Mapbox Tileset Source

This script replaces a Mapbox tileset source with new GeoJSON data using the Mapbox Tiling Service (MTS).

The script calls the Mapbox **Replace a tileset source** endpoint:

- `PUT https://api.mapbox.com/tilesets/v1/sources/{username}/{id}`

and returns the JSON response from Mapbox, for example:

```json
{
  "file_size": 10592,
  "files": 1,
  "id": "mapbox://tileset-source/username/hello-world",
  "source_size": 10592
}
```

See more information about the relevant endpoint from the Mapbox Tiling Service API in the [Mapbox documentation](https://docs.mapbox.com/api/maps/mapbox-tiling-service/#replace-a-tileset-source).

> [!WARNING] 
>
> Tileset processing and hosting are billed separately by Mapbox. The free tier includes only a limited amount of tileset processing and hosting; beyond that, charges apply per processed km² and per stored km²·day. Before scheduling this script as a recurring job in Windmill, review the Mapbox tileset processing pricing and limits carefully:
> [Mapbox tileset processing pricing](https://www.mapbox.com/pricing#tileset-processing)  
> After scheduling, monitor your Mapbox usage to avoid unexpected charges when running frequent or large updates.

