# iNaturalist

[**iNaturalist**](https://www.inaturalist.org/) is a citizen-science platform for recording and identifying biodiversity observations. Communities and projects use it to document species sightings with photos, taxonomy, and location data.

## `inaturalist_pull.py`

Fetches public observations via the [iNaturalist API](https://api.inaturalist.org/v1/docs/) for either a **project** or a **user**. Saves raw JSON and GeoJSON to the datalake, writes features to PostgreSQL, and downloads photo attachments to `{attachment_root}/{db_table_name}/attachments/` (original size; files already on disk are skipped).

> [!IMPORTANT]
> This script only imports **publicly visible** observations. Private locations,
> obscured true coordinates, and other non-public fields are not available without
> authentication.

### Parameters

* **source** — `"project"` or `"user"`.
* **slug** — when `source` is `"project"`, the project numeric ID or slug; when `"user"`, the iNaturalist username.

Project URLs:

* `https://www.inaturalist.org/projects/{slug}`
* `https://www.inaturalist.org/projects/{id}`

Example: [Lake Accotink Park](https://www.inaturalist.org/projects/lake-accotink-park) → slug `lake-accotink-park` or `13795`, source `project`.

User profile URLs:

* `https://www.inaturalist.org/people/{username}`

Example: `https://www.inaturalist.org/people/field_observer` → slug `field_observer`, source `user`.

For projects, this connector uses `project_id` (observations associated with the project), not `apply_project_rules_for`. Filtering by username does not require authentication and does not prove you are that user.

### Notes

* Pagination uses observation ID cursors (`id_above`) rather than page numbers, as recommended by iNaturalist for large result sets.
* The script stays at or below ~60 requests per minute between paginated API calls, and pauses briefly between photo downloads.
* Photos are saved as `{photo_id}.{ext}` under `attachments/`. The first photo's local name is also stored as `photo_filename` on each feature.
* Observations without visible coordinates are still stored with null geometry.

## Future work: supporting private or obscured coordinates

Supporting private or obscured coordinates would require registering an iNaturalist application, completing OAuth2 to obtain an access token, exchanging it for a JWT via `/users/api_token`, and sending that JWT on API requests. JWTs expire after about 24 hours, so a long-lived integration would need refresh logic.

## 📚 Reference

* [iNaturalist API documentation](https://api.inaturalist.org/v1/docs/)
* [iNaturalist Getting Started](https://www.inaturalist.org/pages/getting+started)
