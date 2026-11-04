# Ingest

`Ingest` helps connectors save files to the datalake, write data to the
warehouse, and record what happened in the Registry. GBIF is the first
connector to use it.

The connector still fetches and prepares the data. `Ingest` takes care of
saving the results and updating the Registry. The Registry tables need to be
installed separately before you run the connector.

Here's what the GBIF import looks like once its files are ready:

```python
from f.common_logic.ingest import Ingest

with Ingest(db, "gbif_occurrences", attachment_root) as ingest:
    ingest.write_file(archive, "download.zip", metadata=provenance, dataset_source=True)
    ingest.write_file(csv_path, "download.csv", dataset_source=True)
    ingest.write_file(provenance_path, "download.json")
    ingest.write_dataset(
        dataset=table_name,
        source=csv_path,
        identity="_id",
        record_count=record_count,
        timestamp_column="observations_last_imported_at",
    )
```

`write_file` adds a prepared file to the list of files to save. Nothing is
published yet. `write_dataset` saves those files and imports the CSV, matching
existing rows by the `identity` column. Matching rows are updated; new rows
are added.

Use `dataset_source=True` for files that supply the dataset, such as the
original archive and the CSV. Other files, like the provenance JSON, are
recorded as outputs of the run. The `timestamp_column` argument keeps GBIF's
existing import-time table working for its current consumers.

## If an import fails

Warehouse data and Registry updates are saved in one database transaction.
If either fails, both are rolled back. Files are saved first, though, so they
can remain on disk after a failed import.

An existing Registry file needs a checksum before it can be replaced. If it
has none, the import stops so we don't guess whether its contents changed.

## Where we're starting

This version supports GBIF's CSV import. Connectors still choose filenames,
and `Ingest` doesn't yet follow files moved or renamed through the file
browser. Dataset Importer v2 and additional import strategies can be brought
into the shared code as we migrate more connectors.
