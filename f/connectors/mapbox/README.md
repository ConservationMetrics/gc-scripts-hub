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

The recipe uses zoom levels 0 to a configurable maximum (default: 11, valid range: 0–16) and creates a single layer named after the tileset ID (with hyphens replaced by underscores). The maximum zoom level can be configured via the `max_zoom` parameter.

> ![TIP]
>
> You can use [OpenStreetMap's Zoom levels](https://wiki.openstreetmap.org/wiki/Zoom_levels) guide to determine the appropriate zoom level for your tileset.

### Endpoints

See information about endpoints used in the Mapbox Tiling Service API docs:
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

See information about endpoints used in the Mapbox Tiling Service API docs:
- [Replace a tileset source](https://docs.mapbox.com/api/maps/mapbox-tiling-service/#replace-a-tileset-source)
- [Publish a tileset](https://docs.mapbox.com/api/maps/mapbox-tiling-service/#publish-a-tileset)


## TODO

- [ ] Use Mapbox credentials resource type (when approved) https://hub.windmill.dev/resource_types/340/mapbox_credentials
- [ ] Consider using Datasets API as a prior step to creating a tileset, if we determine that using [Mapbox Datasets](https://docs.mapbox.com/studio-manual/reference/datasets/) has a value add for a given use case (for example, so it can be downloaded as GeoJSON from Mapbox Studio in the future).
