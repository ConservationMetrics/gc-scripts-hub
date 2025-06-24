# 📦 Download all data (aka "Exit Plan")

As part of our commitment to data sovereignty, we want to ensure that users of GuardianConnector can retrieve their data at any time.

The scripts in this directory provide a basic implementation of such an "exit plan" — a way for users to download the contents of their **project data**. They do not currently include exports of things like:

- Configuration data (e.g., GC Explorer metadata tables, Superset dashboards)
- Windmill config or logs
- CoMapeo Cloud raw volume data
- CapRover platform or service config

For a discussion about what we have chosen to prioritize for export (or alternatively handle via a VM backup), see https://github.com/ConservationMetrics/gc-scripts-hub/issues/95

## `download_all_postgres_data`

This export script targets **project-specific tabular data** stored in the `warehouse` PostgreSQL database:

- Tables ingested via connector scripts (e.g., Kobo, Comapeo, alerts)
- Manually uploaded tabular data

These tables are exported as CSV files, zipped into a single `.zip` archive, and saved to a volume mount location accessible to the user (e.g. `/persistent-storage/datalake/exports/`).

## Usage Notes

- These scripts is intended to be run from **within an active GuardianConnector deployment**, using Windmill.
- Users should be advised **not to run exports while new data is actively being posted** to the warehouse, to avoid inconsistent snapshots.
- Archives are stored under `/persistent-storage/datalake/exports/` by default and can be accessed for download from there, using a tool like [Filebrowser](https://filebrowser.org/).

## Next Steps

Potential planned future improvements include:

- Streaming archive output to object storage (e.g., S3 or Azure Blob)
- Optionally including datalake file attachments in the archive