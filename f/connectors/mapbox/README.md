# Mapbox connectors

Scripts for working with Mapbox tilesets and vector tile data.

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

For this script to work, you need to provide a Mapbox secret access token with scope to work with tilesets. You can create a new secret access token in Mapbox Studio by:

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
- The maximum zoom level is configurable via the `max_zoom` parameter (default: `11`, string pattern-limited to `0-16`).
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

- **Datasets:** In the future, we may consider using the Datasets API as a preliminary step before creating a tileset, if using [Mapbox Datasets](https://docs.mapbox.com/studio-manual/reference/datasets/) provides additional value for a given use case (for example, enabling GeoJSON downloads from Mapbox Studio).
- **Windmill Flow**: We could consider chaining this script to another connector (like the [ArcGIS Download Feature Layer script](../arcgis/README.md)) to create a flow that:
  - downloads and stores a feature layer from ArcGIS Online
  - publishes the tileset to Mapbox.

# `mapbox_download_vector_tiles`: Download Vector Tiles to GeoJSON

Download Mapbox vector tiles for a bounding box into a local cache, then convert them into a single GeoJSON FeatureCollection.

Flow:

1. Plan the tile grid for the requested bbox and zoom (with a hard zoom cap for quota safety).
2. If `dry_run` is true, return the plan and stop — no network requests.
3. Otherwise download uncached tiles, convert cached PBFs to GeoJSON (optionally reconstructing clipped features), and write the result to the datalake.

## Intended use case

Use this when you need vector features from a Mapbox tileset as GeoJSON for analysis, archival, or a downstream workflow:

1. Run with `dry_run=true` to confirm the tile count is within your Mapbox API budget.
2. Run again with `dry_run=false` to download and convert into GeoJSON under the datalake.

## Parameters

| Parameter | Default | Description |
|---|---|---|
| `mapbox` | *(required)* | Mapbox credentials resource. Only `access_token` is used. |
| `bbox` | *(required)* | Bounding box as `minlon,minlat,maxlon,maxlat` (WGS84 degrees). |
| `tileset` | *(required)* | Mapbox tileset id (e.g. `mapbox.mapbox-streets-v8`). |
| `zoom` | `12` | Zoom level of the tile grid (string pattern, limited to 0-14). |
| `reconstruct_column` | `""` | Feature property used to reconstruct features clipped across tile boundaries. Empty disables reconstruction. |
| `output_filename` | `my_filename` | Base name for the GeoJSON file and tile cache directory. |
| `attachment_root` | `/persistent-storage/datalake/mapbox` | Datalake directory for output. |
| `delete_tiles` | `true` | After a successful GeoJSON write, delete the tile cache so only the GeoJSON remains. Set `false` to keep tiles for faster re-runs. |
| `dry_run` | `false` | Return the download plan without fetching or writing GeoJSON. |

### Output paths

- Tile cache: `{attachment_root}/{output_filename}/tiles/{z}/{x}/{y}.pbf`
- GeoJSON: `{attachment_root}/{output_filename}.geojson`

Cached tiles are skipped on re-run (when `delete_tiles` is false). Empty cells are cached as zero-byte markers for both HTTP 204 (blank tile) and HTTP 404 `Tile not found` (sparse coverage / outside the tileset extent). Other failures (auth, rate limit, missing tileset, etc.) are not cached, so re-running retries only those.

## Access token

Unlike `mapbox_create_or_update_tileset`, this script only needs a token that can read vector tiles. A **public** token (`pk.ey...`) with access to the target tilesets is sufficient; a secret token also works. Create or copy a token from **Admin > Tokens** in Mapbox Studio.

## Quota guardrails

Vector tile requests count against Mapbox API quota:

- **Dry-run** — inspect `tile_count` / `to_fetch_count` before spending quota.
- **Local cache** — already-downloaded tiles are never re-requested.
- **Zoom limit** — tile count grows ~4× per zoom level; `zoom` is capped at 14 in the Windmill schema.

> [!TIP]
>
> Use [OpenStreetMap's Zoom Levels guide](https://wiki.openstreetmap.org/wiki/Zoom_levels) to pick an appropriate zoom. A tileset only contains data up to its own `maxzoom`; higher zooms often mean more requests with no extra detail.

## Endpoints

- [Retrieve vector tiles](https://docs.mapbox.com/api/maps/vector-tiles/)