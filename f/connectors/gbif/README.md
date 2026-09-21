# `gbif_download`: GBIF occurrences

Run the `f/connectors/gbif/gbif_download` Flow for an initial backfill or an
ad-hoc update. It submits a GBIF asynchronous SIMPLE_CSV download, waits
passively for completion, preserves the archive and citation metadata, then
upserts its occurrences into PostgreSQL.

Create a Windmill `c_gbif` resource manually (custom resource types cannot be
synced): `{"username":"string","password":"string"}`. Use the GBIF
**username**, not an email address. Never place credentials in Flow inputs or
logs. Register an account and review [GBIF download API restrictions](https://techdocs.gbif.org/en/data-use/api-downloads), including its load-dependent limit on incomplete downloads per account.

Bounds must be `[[west, south], [east, north]]` in longitude/latitude order,
for example `[[-55.03, 3.23], [-54.12, 3.67]]`. Only non-antimeridian boxes of
approximately 15,000 km2 or less are accepted. `max_months_lookback` filters
GBIF `LAST_INTERPRETED`, not observation date; omit it for the initial
backfill. For monthly updates, use a two-month overlap and use a wider
backfill after longer gaps.

Archives, converted CSV, and key/DOI/license/predicate provenance are stored
under `{attachment_root}/{db_table_name}/`. The converted CSV and PostgreSQL
columns use snake_case; `_id` duplicates the retained `gbif_id` value. Valid
coordinates become longitude-first Point geometry. Imports are upsert-only, so
records deleted by GBIF or moved outside the territory are not removed locally.

Recommended schedule: `0 0 3 1 * *` (Windmill six-field cron, monthly on the
first at 03:00 **UTC**). The Flow checks status every 60 seconds for at most 24
hours without retaining a Python worker. Downloading/importing still use real
jobs and may exceed a Windmill 30-minute default; operators may need to raise
the instance maximum because a step timeout cannot exceed it.

If submission times out, do not resubmit automatically: inspect the Windmill
run and the GBIF account downloads for the retained key. Terminal status or an
expired deadline stops before ingestion. The shared CSV writer upserts rows but
does not report reliable inserted-row counts or make the import atomic.
