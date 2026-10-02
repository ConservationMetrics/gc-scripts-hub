# GBIF connectors

Two separate entry points:

- **`gbif_download`** downloads occurrence records for an area and upserts them
  into PostgreSQL. Use it for an initial backfill or a recurring update.
- **`gbif_pull_statistics`** replaces one table with GBIF's facet counts for an
  area. It can be run on its own, without downloading occurrences.

The two are not meant to describe the same set of rows. The download can limit
records with `max_months_lookback` and upserts what it imports. The statistics
script counts every occurrence GBIF currently indexes in the bounding box, and
it replaces its table on each run.

## `gbif_download`

### Setup

Create a Windmill `gbif` resource with your GBIF username and password:

```json
{ "username": "string", "password": "string" }
```

Use your GBIF username, not your email address. Keep these credentials out of
Flow inputs and logs. Review the [GBIF download API restrictions](https://techdocs.gbif.org/en/data-use/api-downloads)
before running downloads.

### Parameters

- **`bounding_box`** — Two corners in longitude/latitude order:
  `[[west, south], [east, north]]`. Boxes must not cross the antimeridian and
  must be approximately 15,000 km2 or smaller.
- **`max_months_lookback`** — Optional number of months to look back. This
  filters GBIF's `LAST_INTERPRETED` date, not the observation date. Omit it for
  an initial backfill.
- **`db`** — PostgreSQL resource for the occurrence table.
- **`db_table_name`** — Destination table name and datalake subdirectory.
  The name can be at most 53 characters. The last import time is saved in
  `{db_table_name}__metadata`.
- **`attachment_root`** — Directory for downloaded files. Defaults to
  `/persistent-storage/datalake`.

> [!TIP]
> Use [Mapbox Location Helper](https://labs.mapbox.com/location-helper) to
> choose an area and copy its viewport bounds into `bounding_box`.

### Stored files and data

Files are stored under
`{attachment_root}/{db_table_name}/`:

- The original GBIF archive
- A converted CSV
- Provenance metadata, including the download key, DOI, license, and query

The CSV and PostgreSQL columns use `snake_case`. The `_id` column contains the
GBIF `gbif_id`, and records with valid coordinates include Point geometry. The
`dataset` and `publishing_org` columns contain human-readable titles resolved
from GBIF's Registry API. Their corresponding `dataset_key` and
`publishing_org_key` UUID columns are retained.

Registry titles are resolved once per distinct key in each download. If GBIF's
Registry API is unavailable or a key has no title, the import continues with an
empty title while preserving the UUID. Registry enrichment stops starting new
lookup batches after two minutes, and each active request has connect and read
timeouts. The provenance file records how many dataset and publishing
organization keys were found and resolved.

Imports use upserts. Existing records are updated, but records deleted by GBIF
or moved outside the selected area are not removed from PostgreSQL.

Each successful import replaces `{db_table_name}__metadata` with one row:

| observations_last_imported_at |
| ----------------------------- |
| 2026-10-02T00:12:00+00:00     |

### Scheduling and failures

For monthly updates, use the Windmill cron schedule `0 0 3 1 * *` (the first
day of each month at 03:00 UTC). Use a two-month lookback to reduce gaps
between runs.

If submission times out, check the Windmill run and the GBIF account's download
list before trying again. The download may have been accepted even if the
submission request timed out.

The Flow checks GBIF every 60 seconds and has a hardcoded maximum wait of 24
hours. If the download has not finished by then, the Flow stops before
importing it. This limit prevents a stuck or unusually slow GBIF download from
holding a Windmill job indefinitely. Large downloads may also need a Windmill
job timeout longer than the 30-minute default.

## `gbif_pull_statistics`

`gbif_pull_statistics` takes the same style of bounding box and writes facet
counts into `db_table_name` itself. The name can be at most 63 characters. The
area must be approximately 35,000 km2 or smaller.

Each row is one facet value. `key` is the GBIF identifier and `label` is the
display name, so two datasets, publishers, or species that share a title stay
separate rows.

| facet                  | key                                  | label                            | count |
| ---------------------- | ------------------------------------ | -------------------------------- | ----: |
| dataset                | 7a3679ef-5582-4aaa-81f0-8c2545cafc81 | Pl@ntNet observations            |    60 |
| publisher              | 28eb1a3f-1c15-4a95-931a-4af90ecb574d | iNaturalist.org                  |    60 |
| year                   | 2026                                 |                                  |    70 |
| species                | 2474363                              | Psophia crepitans Linnaeus, 1758 |    55 |
| basis_of_record        | HUMAN_OBSERVATION                    |                                  |    80 |
| statistics_imported_at | 2026-10-02T00:33:00+00:00            |                                  |       |

Dataset and publisher labels are GBIF Registry titles. Species labels are
scientific names. Years and basis of record have no separate label. When a
Registry lookup fails, `label` is empty and `key` still identifies the row.
`statistics_imported_at` is rewritten each time the counts are saved. Filter
with `WHERE facet = 'species'`. Each run replaces the table with the current
GBIF index for the area, including occurrences outside any download lookback.
