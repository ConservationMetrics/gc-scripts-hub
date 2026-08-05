# iNaturalist

[**iNaturalist**](https://www.inaturalist.org/) is a citizen-science platform for recording and identifying biodiversity observations. Communities and projects use it to document species sightings with photos, taxonomy, and location data.

These scripts fetch public observations via the [iNaturalist API](https://api.inaturalist.org/v1/docs/), save raw JSON and GeoJSON to the datalake, and write features to PostgreSQL. Photo URLs are stored as properties; photo binaries are not downloaded.

> [!IMPORTANT]
> These scripts currently only import **publicly visible** observations. Private locations,
> obscured true coordinates, and other non-public fields are not available without
> authentication.

## `inaturalist_pull_project.py`

Fetches observations associated with an iNaturalist **project**, plus project metadata.

### Parameters

The **project ID** may be either the numeric project ID or the project slug. Both appear in the project URL:

* `https://www.inaturalist.org/projects/{slug}`
* `https://www.inaturalist.org/projects/{id}`

Example: for [Lake Accotink Park](https://www.inaturalist.org/projects/lake-accotink-park), use `lake-accotink-park` or `13795`.

This connector uses `project_id` (observations associated with the project), not `apply_project_rules_for`.

## `inaturalist_pull_my_observations.py`

Fetches observations for a given iNaturalist **username**. No authentication is required for publicly visible observations belonging to that account—filtering by username does not prove you are that user.

### Parameters

The **username** is the profile slug from the people URL:

* `https://www.inaturalist.org/people/{username}`

Example: `https://www.inaturalist.org/people/field_observer` → `field_observer`.

## Shared notes

* Pagination uses observation ID cursors (`id_above`) rather than page numbers, as recommended by iNaturalist for large result sets.
* The scripts stay at or below ~60 requests per minute between paginated calls.
* Observations without visible coordinates are still stored with null geometry.

## Future work: supporting private or obscured coordinates

Supporting private or obscured coordinates would require registering an iNaturalist application, completing OAuth2 to obtain an access token, exchanging it for a JWT via `/users/api_token`, and sending that JWT on API requests. JWTs expire after about 24 hours, so a long-lived integration would need refresh logic.

## 📚 Reference

* [iNaturalist API documentation](https://api.inaturalist.org/v1/docs/)
* [iNaturalist Getting Started](https://www.inaturalist.org/pages/getting+started)
