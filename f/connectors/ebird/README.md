# eBird

[**eBird**](https://ebird.org/) is the Cornell Lab of Ornithology's database of bird observations. `ebird_pull.py` downloads an observation list from the [eBird API 2.0](https://documenter.getpostman.com/view/664302/S1ENwy59) and writes it to the datalake and PostgreSQL.

## `ebird_pull.py`

The **Observations** dropdown selects one endpoint:

1. **Recent observations in a region** — `GET /v2/data/obs/{regionCode}/recent`
2. **Recent notable observations in a region** — `GET /v2/data/obs/{regionCode}/recent/notable?detail=full`
3. **Recent nearby observations** — `GET /v2/data/obs/geo/recent?lat=&lng=`
4. **Recent nearby notable observations** — `GET /v2/data/obs/geo/recent/notable?lat=&lng=&detail=full`
5. **Historic observations on a date** — `GET /v2/data/obs/{regionCode}/historic/{year}/{month}/{day}`

Each run is a single request. Region and nearby results are the most recent observation of each species. Notable observations are locally or nationally rare, or otherwise unusual, and can include more than one row for the same species. Notable requests send `detail=full`, which adds observer and place-name fields.

The API token is sent as the `X-eBirdApiToken` header. **API token** is a Windmill variable: create the key once as a secret variable, then select that variable on the run or schedule. Request a key at [ebird.org/api/keygen](https://ebird.org/api/keygen).

> [!NOTE]
> Unlike the [GBIF](../gbif/README.md) and [iNaturalist](../inaturalist/README.md) connectors, eBird cannot be searched by a specific geography such as a bounding box. A subnational region must be used.

> [!NOTE]
> It is not possible to batch-download all historical observations. **Historic observations on a date** returns one day at a time: enter that day as `YYYY-MM-DD` with a region code. Recent observation lists cover at most the last 30 days, and those requests use the API default of 14 days.

> [!TIP]
> Use the [Region Info](https://documenter.getpostman.com/view/664302/S1ENwy59#07c64240-6359-4688-9c4f-ff3d678a7248) endpoint to look up a subnational code. For Suriname (`SR`):
>
> ```bash
> curl --location 'https://api.ebird.org/v2/ref/region/list/subnational1/SR' \
>   --header 'X-eBirdApiToken: YOUR_API_TOKEN'
> ```
>
> Copy a `code` from the response into **Region code**.

> [!TIP]
> [Statoids](https://statoids.com/) also lists subnational region codes. Replace the dot with a hyphen. Sipaliwini is `SR.SI` on Statoids and `SR-SI` in eBird.

### Parameters

- **api_token** — Windmill variable holding the eBird API token. Create a secret variable on the Variables page, then select it here. The selection is what gets saved on a run or schedule, so the token is not re-entered each time.
- **observation_type** — one of the five lists above.
- **region_code** — shown for the region options and for historic observations on a date. An eBird country, subnational1, subnational2, or location code, such as `US-NY`, `US-NY-071`, or `L295658`.
- **observation_date** — shown for historic observations on a date. A single day as `YYYY-MM-DD`, from `1800-01-01` through today. Example: `2020-01-21`.
- **lon_lat** — shown for the two nearby options. Longitude and latitude separated by a comma, for example `79.4551, 43.5309`. The form accepts numbers only. Longitude must be from -180 to 180 and latitude from -90 to 90.
- **destination_action** — `use_existing_dataset` or `create_new_dataset` (default). The form shows these as "Use existing dataset" and "Enter dataset name" under **Dataset**. Entering a name creates the dataset if needed and updates it if it already exists.
- **existing_db_table_name** — public tables on `db`, offered as a dynamic select when using an existing dataset. Sidecar tables (`__columns`, `__labels`, `__metadata`) are omitted.
- **db_table_name** — new table name when creating a dataset. At most 54 characters. Also the datalake subdirectory.
- **attachment_root** — directory for saved files. Defaults to `/persistent-storage/datalake`.

Nearby queries use eBird's default search radius of 25 km. The API allows at most 50 km.

### Stored files and columns

Files are stored under `{attachment_root}/{db_table_name}/`:

- `{db_table_name}_observations.json` — the API response
- `{db_table_name}.geojson` — one Point per observation, coordinates `[longitude, latitude]`

`_id` is `obsId` when the response includes it. Recent and historic observations use the simple response, which has no `obsId`, so `_id` is `{subId}_{speciesCode}`.

Columns are `snake_case`: `species_code`, `common_name`, `scientific_name`, `loc_id`, `loc_name`, `observed_at`, `how_many`, `lat`, `lng`, `obs_valid`, `obs_reviewed`, `location_private`, `sub_id`, plus `data_source` (`eBird`) and `observation_type`. Notable records also fill `subnational1_code`, `subnational1_name`, `subnational2_code`, `subnational2_name`, `country_code`, `country_name`, `user_display_name`, `obs_id`, `checklist_id`, `presence_noted`, `has_comments`, `observer_first_name`, `observer_last_name`, and `has_rich_media`. Recent and historic records leave those columns null.

Region and historic pulls add `region_code`. Historic pulls also add `observation_date`, the `YYYY-MM-DD` day requested. Nearby pulls add `search_lon` and `search_lat`, the point from **Longitude, latitude**. That point is separate from each observation's `lng` and `lat`.

`location_private` is eBird's privacy flag. Coordinates are stored as the API returns them, including when that flag is true. These endpoints do not return media files; `has_rich_media` is only a flag. An empty response does not create a table.

eBird caps these endpoints at 10,000 observations per request.

## Terms of use

Review the [eBird API Terms of Use](https://www.birds.cornell.edu/home/ebird-api-terms-of-use/) before storing or sharing observations. Attribute the records to eBird.

## Reference

- [eBird API 2.0](https://documenter.getpostman.com/view/664302/S1ENwy59)
- [Request an eBird API key](https://ebird.org/api/keygen)
- [eBird API Terms of Use](https://www.birds.cornell.edu/home/ebird-api-terms-of-use/)
- [Region Info](https://documenter.getpostman.com/view/664302/S1ENwy59#07c64240-6359-4688-9c4f-ff3d678a7248)
- [Historic observations on a date](https://documenter.getpostman.com/view/664302/S1ENwy59#2d8c6ee8-c435-4e91-9f66-6d3eeb09edd2)
