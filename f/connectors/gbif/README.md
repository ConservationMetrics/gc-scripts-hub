# `gbif_download`: Download GBIF occurrences

Runs a GBIF occurrence download and imports the results into PostgreSQL. The
Flow is suitable for an initial backfill or a recurring update.

## Setup

Create a Windmill `c_gbif` resource with your GBIF username and password:

```json
{"username":"string","password":"string"}
```

Use your GBIF username, not your email address. Keep these credentials out of
Flow inputs and logs. Review the [GBIF download API restrictions](https://techdocs.gbif.org/en/data-use/api-downloads)
before running downloads.

## Parameters

- **`bounding_box`** — Two corners in longitude/latitude order:
  `[[west, south], [east, north]]`. Boxes must not cross the antimeridian and
  must be approximately 15,000 km2 or smaller.
- **`max_months_lookback`** — Optional number of months to look back. This
  filters GBIF's `LAST_INTERPRETED` date, not the observation date. Omit it for
  an initial backfill.
- **`db`** — PostgreSQL resource for the occurrence table.
- **`db_table_name`** — Destination table name and datalake subdirectory.
- **`attachment_root`** — Directory for downloaded files. Defaults to
  `/persistent-storage/datalake`.

> [!TIP]
> Use [Mapbox Location Helper](https://labs.mapbox.com/location-helper) to
> choose an area and copy its viewport bounds into `bounding_box`.

## Stored files and data

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

## Scheduling and failures

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
