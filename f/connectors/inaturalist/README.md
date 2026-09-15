# iNaturalist

[**iNaturalist**](https://www.inaturalist.org/) is a citizen-science platform for recording and identifying biodiversity observations. Communities and projects use it to document species sightings with photos, taxonomy, and location data.

## `inaturalist_pull.py`

Fetches public observations via the [iNaturalist API](https://api.inaturalist.org/v2/docs/) for a **project**, a **user**, and/or a geographic **bounding box**. At least one of `slug` or `bounding_box` must be supplied; when both are supplied they are combined on the same query. Saves curated JSON and GeoJSON to the datalake, writes features to PostgreSQL, and downloads photo and sound attachments to `{attachment_root}/{db_table_name}/attachments/`. Files already on disk are skipped.

Observations are requested from **API v2** with an explicit `fields` spec (`_OBSERVATION_FIELDS`). v2 silently ignores unknown field names (HTTP 200, no error), so that dict is the single source of truth for both the request and the table columns. The on-disk `{db_table_name}_observations.json` is this curated archive, not a raw v1 dump.

Project metadata still uses **API v1** (`/projects/{slug}`). v2's projects endpoint rejects slugs (`422 must be integer`).

> [!IMPORTANT]
> This script only imports **publicly visible** observations. Private locations,
> obscured true coordinates, and other non-public fields are not available without
> authentication. When iNaturalist obscures a point, the table stores the public
> (fuzzed) geometry plus `obscured`, `geoprivacy`, `taxon_geoprivacy`,
> `positional_accuracy`, and `public_positional_accuracy` so the fuzzing is
> visible.

### Parameters

- **source** — `"project"` or `"user"`. Required when `slug` is provided; unused for bounding-box-only pulls.
- **slug** — _(optional)_ when `source` is `"project"`, the project numeric ID or slug; when `"user"`, the iNaturalist username.
- **bounding_box** — _(optional)_ JSON string of viewport bounds, `[[west, south], [east, north]]` (longitude, latitude), same idea as the GFW connector. Combined with `slug` when both are set.

Either `slug` or `bounding_box` must be provided.

> [!TIP]
> Use [Mapbox Location Helper](https://labs.mapbox.com/location-helper/) to pan and zoom to an area, then copy the viewport bounds and paste them into `bounding_box`.

Project URLs:

- `https://www.inaturalist.org/projects/{slug}`
- `https://www.inaturalist.org/projects/{id}`

Example: [Lake Accotink Park](https://www.inaturalist.org/projects/lake-accotink-park) → slug `lake-accotink-park` or `13795`, source `project`.

User profile URLs:

- `https://www.inaturalist.org/people/{username}`

Example: `https://www.inaturalist.org/people/field_observer` → slug `field_observer`, source `user`.

A project may itself filter quality grade (Lake Accotink Park uses `research,needs_id`), so a project pull is not the same population as an unfiltered user pull. Filtering by username does not require authentication and does not prove you are that user.

### Observation columns

Identity and provenance: `_id` (numeric observation ID), `uuid`, `uri`, `updated_at`, `observer`, `observer_id`, `observer_name`, `observer_orcid`, `license_code`, `description`, `gbif_occurrence_id` (from `outlinks` where `source` is `GBIF`).

Research Grade determinants: `observed_on`, `time_observed_at`, `observed_time_zone`, `quality_grade`, `captive`, `taxon_rank`, `taxon_rank_level`, `iconic_taxon_name`, `positional_accuracy`, `public_positional_accuracy`, `obscured`, `geoprivacy`, `taxon_geoprivacy`, `mappable`, `place_guess`, identification agreement counts, `photo_count`, `sound_count`.

Media: `photo_filename` / `photo_url` (first photo, unchanged), `photo_filenames` / `sound_filenames` (comma-separated), `photo_license_code`, `photo_attribution`.

### Notes

- Pagination uses observation ID cursors (`id_above`) rather than page numbers, as recommended by iNaturalist for large result sets.
- The script stays at or below ~60 requests per minute between paginated API calls, and pauses briefly between media downloads.
- Photos are saved as `{photo_id}.{ext}` and sounds as `{sound_id}.{ext}` under `attachments/`.
- Observations without visible coordinates are still stored with null geometry.
- iNaturalist projects, or bounding boxes, can yield a huge amount of data! It is possible that when running this script, Windmill will time out after a default of 30 minutes. If that is the case, you will need to increase instance-wide job timeout settings to a sane higher value, and restart the Windmill web app.

## Future work: supporting private or obscured coordinates

Supporting private or obscured coordinates would require registering an iNaturalist application, completing OAuth2 to obtain an access token, exchanging it for a JWT via `/users/api_token`, and sending that JWT on API requests. JWTs expire after about 24 hours, so a long-lived integration would need refresh logic.

## Terms of use

iNaturalist's [Terms of Use](https://www.inaturalist.org/pages/terms) cover the website, apps, and API. This connector only fetches **public** observations through the documented API, stays within their [recommended request rates](https://www.inaturalist.org/pages/api+recommended+practices), and does **not** use iNaturalist data to train commercial AI or ML models (the main prohibition in those terms). Observation and media licenses are per-record: contributors retain rights, and the default is [CC BY-NC](https://creativecommons.org/licenses/by-nc/4.0/) unless they chose otherwise. Keep `license_code`, `photo_license_code`, and `photo_attribution` with the data.

## 📚 Reference

- [iNaturalist Terms of Use](https://www.inaturalist.org/pages/terms)
- [iNaturalist API Recommended Practices](https://www.inaturalist.org/pages/api+recommended+practices)
- [iNaturalist API v2 documentation](https://api.inaturalist.org/v2/docs/)
- [iNaturalist API v1 documentation](https://api.inaturalist.org/v1/docs/) (project metadata)
- [iNaturalist Getting Started](https://www.inaturalist.org/pages/getting+started)
