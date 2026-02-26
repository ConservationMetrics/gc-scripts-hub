# `mapbox_replace_tileset_source`: Replace a Mapbox Tileset Source

This script uses the Mapbox Tiling Service (MTS) to:

1. replace a Mapbox tileset source with new GeoJSON data, and
2. publish the tileset to trigger a rebuild.

> [!NOTE]
>
> This script can only be used with tilesets that were created using the MTS via the `/tilesets/v1` endpoint. That is, it will **not** work with tilesets created directly in Mapbox Studio.

> [!WARNING] 
>
> Tileset processing and hosting are billed separately by Mapbox. The free tier includes only a limited amount of tileset processing and hosting; beyond that, charges apply per processed km² and per stored km²·day. Before scheduling this script as a recurring job in Windmill, review the Mapbox tileset processing pricing and limits carefully:
> [Mapbox tileset processing pricing](https://www.mapbox.com/pricing#tileset-processing)  
> Additionally after scheduling, it is recommended to monitor your Mapbox usage to avoid unexpected charges when running frequent or large updates.

## Endpoints

- **Replace a tileset source**: `PUT https://api.mapbox.com/tilesets/v1/sources/{username}/{id}`

```json
{
  "file_size": 10592,
  "files": 1,
  "id": "mapbox://tileset-source/username/hello-world",
  "source_size": 10592
}
```

- **Publish a tileset**: `POST https://api.mapbox.com/tilesets/v1/{username}.{tileset_id}/publish`

```json
{
  "jobId": "cmm3loss5002x0gjx4lmt8lwf",
  "message": "Processing username.hello-world"
}
```

See more information about these endpoints in the Mapbox Tiling Service API docs:
- [Replace a tileset source](https://docs.mapbox.com/api/maps/mapbox-tiling-service/#replace-a-tileset-source)
- [Publish a tileset](https://docs.mapbox.com/api/maps/mapbox-tiling-service/#publish-a-tileset)