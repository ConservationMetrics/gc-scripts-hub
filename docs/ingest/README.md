# Ingest

`f.common_logic.ingest.Ingest` provides the connector-facing boundary for the
GBIF occurrence import. It records a registry run, publishes files within the
configured datalake, and writes a CSV dataset and its registry metadata.
Deploy the registry schema separately before running a connector that uses it.

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

`write_file` queues a staged file; `write_dataset` publishes the queued files
and merges the CSV by its identity column. The `dataset_source` flag controls
whether the file is linked to the dataset. The timestamp column preserves the
existing GBIF metadata sidecar. The registry and warehouse database changes
commit together. Files are replaced before that transaction and can remain on
disk after failure; retries and reconciliation are necessary. Existing registry
files without checksums must be backfilled before they can be replaced.

File paths currently derive from connector-provided names. The future registry
identity mapping needed to respect file-browser renames is not implemented.
The Dataset Importer v2 review workflow and other write strategies remain
separate; this interface currently implements only GBIF's merge behavior.
