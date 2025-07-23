# 📦 Download all data (aka "Exit Plan")

As part of our commitment to data sovereignty, we want to ensure that users of Guardian Connector can retrieve their data at any time.

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

These tables are exported as CSV files, zipped into a single `.zip` archive, and saved to a volume mount location accessible to the user (e.g. `/persistent-storage/datalake/exports/`). From there, the archive can be downloaded using a tool like [Filebrowser](https://filebrowser.org/) or exported alongside other files via the `download_all_files_azure` script (assuming files are stored in an Azure storage blob).

## `download_all_files_azure`

This export script targets **file-based data** stored in Azure Blob Storage containers:

- Uploaded files (images, documents, media)
- Exported data files from other services
- Any persistent storage files accessible via Azure Blob Storage

The script generates a secure SAS (Shared Access Signature) URL with time-limited access to the specified Azure Blob Storage container or subfolder. It then provides multiple [`azcopy`](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azcopy-v10?tabs=dnf) command options for different destinations:

- Local disk (download to your computer)
- AWS S3 bucket (cloud-to-cloud transfer)
- Google Cloud Storage (cloud-to-cloud transfer)
- Another Azure Storage account (cloud-to-cloud transfer)

This approach using `azcopy` provides several advantages. Transfers are fast, especially when copying data directly between cloud services. You can choose from a variety of destinations for your data, making the process flexible. Because files are transferred directly from Azure storage, there is no additional load on your Guardian Connector deployment. Additionally, if a transfer is interrupted, the `azcopy` tool allows you to resume it without starting over.

The generated SAS URL has a configurable expiry time (default 120 minutes) for security.

## Usage Notes

- These scripts are intended to be run from **within an active Guardian Connector deployment**, using Windmill.
- Users should be advised **not to run exports while new data is actively being posted** to the warehouse, to avoid inconsistent snapshots.

## Next Steps

Potential planned future improvements include:

- Streaming Postgres archive output to blob storage
- A Windmill app to improve the user experience of downloading data
- Export scripts for file-based data stored in other storage systems (AWS S3, Google Cloud Storage, local file systems, etc.), as needed 