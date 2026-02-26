# `mapbox_create_or_update_tileset`: Create or Update a Mapbox Tileset

This script uses the Mapbox Tiling Service (MTS) to create **or** update a Mapbox tileset from a GeoJSON file by:

1. checking if the tileset exists (GET),
2. creating the tileset if missing (404), otherwise updating it (200), and
3. publishing the tileset to (re)build tiles from the current source.

> [!WARNING] 
>
> Tileset processing and hosting are billed separately by Mapbox. The free tier includes only a limited amount of tileset processing and hosting; beyond that, charges apply per processed km² and per stored km²·day. 
> 
> Before scheduling these scripts as recurring jobs in Windmill, review the Mapbox tileset processing pricing and limits carefully:
>  
> - [Mapbox tileset processing pricing](https://www.mapbox.com/pricing#tileset-processing)  
> - [Mapbox tileset billing metrics](https://docs.mapbox.com/help/glossary/tileset-billing-metrics/)
> 
> Additionally, after scheduling, it is recommended to monitor your Mapbox usage to avoid unexpected charges when running frequent or large updates.

## Tileset Recipe

If the tileset is created, the [tileset recipe](https://docs.mapbox.com/mapbox-tiling-service/guides/tileset-recipes/) uses zoom levels 0 to a configurable maximum (default: 11, valid range: 0–16) and creates a single layer named after the tileset ID (with hyphens replaced by underscores). 

The maximum zoom level can be configured via the `max_zoom` parameter (ignored when updating an existing tileset).

> [!TIP]
>
> You can use [OpenStreetMap's Zoom levels](https://wiki.openstreetmap.org/wiki/Zoom_levels) guide to determine the appropriate zoom level for your tileset.

## Endpoints

See information about endpoints used in the Mapbox Tiling Service API docs:
- [Get a tileset](https://docs.mapbox.com/api/maps/mapbox-tiling-service/#get-a-tileset)
- [Create a tileset source](https://docs.mapbox.com/api/maps/mapbox-tiling-service/#create-a-tileset-source)
- [Create a tileset](https://docs.mapbox.com/api/maps/mapbox-tiling-service/#create-a-tileset)
- [Replace a tileset source](https://docs.mapbox.com/api/maps/mapbox-tiling-service/#replace-a-tileset-source)
- [Publish a tileset](https://docs.mapbox.com/api/maps/mapbox-tiling-service/#publish-a-tileset)

## Possible Extensions

**Datasets:** In the future, we may consider using the Datasets API as a preliminary step before creating a tileset, if we determine that using [Mapbox Datasets](https://docs.mapbox.com/studio-manual/reference/datasets/) adds value for a given use case (for example, enabling GeoJSON downloads from Mapbox Studio).

## TODO

- [ ] Use Mapbox credentials resource type (when approved) https://hub.windmill.dev/resource_types/340/mapbox_credentials