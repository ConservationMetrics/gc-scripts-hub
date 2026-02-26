# Mapbox Connector Scripts

This directory contains scripts for working with the Mapbox Tiling Service (MTS).

> [!WARNING] 
>
> Tileset processing and hosting are billed separately by Mapbox. The free tier includes only a limited amount of tileset processing and hosting; beyond that, charges apply per processed km² and per stored km²·day. Before scheduling these scripts as recurring jobs in Windmill, review the Mapbox tileset processing pricing and limits carefully:
> [Mapbox tileset processing pricing](https://www.mapbox.com/pricing#tileset-processing)  
> Additionally after scheduling, it is recommended to monitor your Mapbox usage to avoid unexpected charges when running frequent or large updates.

## `mapbox_create_tileset`: Create a Mapbox Tileset

This script uses the Mapbox Tiling Service (MTS) to create a new Mapbox tileset from a GeoJSON file by:

1. creating a tileset source from the GeoJSON data,
2. creating a tileset with a simple recipe that references that source, and
3. publishing the tileset to begin processing.

The recipe uses fixed zoom levels 0–22 and creates a single layer named after the tileset ID (with hyphens replaced by underscores).

### Endpoints

- **Create a tileset source**: `POST https://api.mapbox.com/tilesets/v1/sources/{username}/{id}`

```json
{
  "id": "mapbox://tileset-source/username/hello-world",
  "files": 1,
  "source_size": 219,
  "file_size": 219
}
```

- **Create a tileset**: `POST https://api.mapbox.com/tilesets/v1/{username}.{tileset_id}`

```json
{
  "message": "Successfully created empty tileset username.hello-world. Publish your tileset to begin processing your data into tiles."
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
- [Create a tileset source](https://docs.mapbox.com/api/maps/mapbox-tiling-service/#create-a-tileset-source)
- [Create a tileset](https://docs.mapbox.com/api/maps/mapbox-tiling-service/#create-a-tileset)
- [Publish a tileset](https://docs.mapbox.com/api/maps/mapbox-tiling-service/#publish-a-tileset)

## `mapbox_update_tileset`: Update a Mapbox Tileset

This script uses the Mapbox Tiling Service (MTS) to update a Mapbox tileset by:

1. replacing the tileset source with new GeoJSON data, and
2. publishing the tileset to trigger a rebuild.

> [!NOTE]
>
> This script can only be used with tilesets that were created using the MTS via the `/tilesets/v1` endpoint. That is, it will **not** work with tilesets created directly in Mapbox Studio.

### Endpoints

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