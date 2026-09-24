# GC Dataset Importer v2

React 19 Windmill full-code app for Dataset Importer v2.

## Goals
Support more complex 'import strategies', where v1 only supported basic merges.
- **Create** creates a dataset from the uploaded records.
- **Append** inserts every uploaded record without matching.
- **Merge** adds unmatched records and retains existing unmatched records.
- **Sync** adds unmatched records and deletes existing unmatched records.

Merge and Sync are possible because we enable the user to select up to three columns to use as "identity" as well as a conflict policy (imported wins, or existing wins).

Complete feature spec with acceptance criteria was written to [./SPEC.md](./SPEC.md)

Successful sources are stored under
`/persistent-storage/datalake/<dataset>`.

## Supported uploads

- CSV, JSON arrays, GeoJSON, GPX, KML, single-layer GeoPackage, XLS, XLSX
- Shapefile ZIPs and ZIPs containing supported files
- CyberTracker backup JSON, detected by content
- SMART patrol XML, detected by namespace

## Known Limitations

- No "replace dataset" option. Decided against it just for simplicity. The difference between "sync" and "replace" is that "sync" retains existing columns even if they don't exist in the newly imported dataset where "replace" would not have. If you want to "replace" today, you can just delete the dataset and create a new one.
- `.geojson` imports with GeometryCollections are not supported because we don't have a valid table schema to support these. We would need `__geometry` instead of `__coordinates`.

## How it works

The React app calls Windmill's app-local `backend/` scripts. These are small
entry points into `f/common_logic/dataset_importer_v2.py`, run as worker jobs;
their YAML binds the PostgreSQL resource, so the browser cannot choose database
credentials. Windmill manages each script's Python dependencies via its lockfile.

1. **Stage:** The browser sends one base64-encoded file (up to 25 MiB). The
   importer parses it and stores the original bytes in PostgreSQL
   `import_sessions`, and parsed rows in `import_rows`. It assigns stable column
   names, including distinct names when source columns collide. No target rows
   are written yet; Create also reserves the new dataset name.
2. **Preview:** PostgreSQL checks the selected identity fields for duplicates
   and calculates additions, updates, deletions, and other review counts using
   SQL. The preview saves a target-data/schema fingerprint and a `preview_id`.
   Only compatible datasets are offered as targets; updates to existing
   non-text columns are rejected during preview.
3. **Apply:** Confirmation sends the `import_id` and `preview_id`, not the file
   again. The worker archives the exact original bytes to the datalake first,
   then locks the target and applies the reviewed changes in a database
   transaction. If the target or preview has changed, confirmation is rejected;
   repeating a successful confirmation does not import twice. A failed database
   write rolls back target changes and attempts to remove the new archive.

Staged imports expire after 24 hours. Cleanup runs on subsequent importer
activity, or can be scheduled separately; it is not a timed background job by
default. If archiving fails, the reviewed session retains the file so the user
can retry confirmation from the open app without re-uploading. Reloading the app
does not restore that review session. Workers need access to the same persistent
datalake storage for archives and cleanup.

Frontend tests mock the Windmill backend; Python tests exercise the importer
against a temporary PostgreSQL database. Neither runs the deployed app through
a Windmill worker, so deployment should include a small import smoke test.
