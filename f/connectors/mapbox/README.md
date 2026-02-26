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
> After scheduling, you should also monitor your Mapbox usage to avoid unexpected charges when running frequent or large updates.

## Intended use case

This script is intended for workflows where a GeoJSON dataset is updated regularly and needs to be reflected in a Mapbox tileset. For example:

- A government agency maintains a protected areas feature layer in ArcGIS Online and updates it quarterly.
- A Guardian Connector user displays this layer in a Mapbox map and wants the tileset to reflect the latest data.
- The user schedules one of the [ArcGIS Download Feature Layer scripts](../arcgis/README.md) to download the updated data from ArcGIS Online and store it as a GeoJSON file.
- The user then schedules the `mapbox_create_or_update_tileset` script to create or update the tileset from that file.
- The Mapbox map will automatically display the updated data once the tileset publish job completes.

## Mapbox Secret Access Token

For the scripts to work, you need to provide a Mapbox secret access token with scope to work with tilesets. You can create a new secret access token in Mapbox Studio by:

1. Navigating to **Admin >  Tokens**
2. Clicking **+ Create a token**
3. Adding the following secret scopes:
    - `tilesets:write`
    - `tilesets:read`
    - `tilesets:delete`
4. Copy down the secret access token value (starting with `sk.ey...`) and save it as you will not be able to access it again after closing the dialog.

## Zoom levels in the Tileset Recipe

When creating a tileset, a [tileset recipe](https://docs.mapbox.com/mapbox-tiling-service/guides/tileset-recipes/) defines parameters such as zoom levels.

In this script:

- The minimum zoom level is hard-coded to `0`.
- The maximum zoom level is configurable via the `max_zoom` parameter (default: `11`, valid range: `0-16`).
- The `max_zoom` parameter is ignored when updating an existing tileset.

> [!TIP]
>
> You can use [OpenStreetMap's Zoom Levels guide](https://wiki.openstreetmap.org/wiki/Zoom_levels) to help determine an appropriate maximum zoom level for your tileset.

## Endpoints

This script uses the following Mapbox Tiling Service API endpoints:

- [Get a tileset](https://docs.mapbox.com/api/maps/mapbox-tiling-service/#get-a-tileset)
- [Create a tileset source](https://docs.mapbox.com/api/maps/mapbox-tiling-service/#create-a-tileset-source)
- [Create a tileset](https://docs.mapbox.com/api/maps/mapbox-tiling-service/#create-a-tileset)
- [Replace a tileset source](https://docs.mapbox.com/api/maps/mapbox-tiling-service/#replace-a-tileset-source)
- [Publish a tileset](https://docs.mapbox.com/api/maps/mapbox-tiling-service/#publish-a-tileset)

## Possible Extensions

**Datasets:** In the future, we may consider using the Datasets API as a preliminary step before creating a tileset, if using [Mapbox Datasets](https://docs.mapbox.com/studio-manual/reference/datasets/) provides additional value for a given use case (for example, enabling GeoJSON downloads from Mapbox Studio).

## TODO

- [ ] Use Mapbox credentials resource type (when approved) https://hub.windmill.dev/resource_types/340/mapbox_credentials