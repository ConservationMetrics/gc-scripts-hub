# GC Dataset Importer POC

React 19 Windmill full-code app for the isolated Dataset Importer POC. The
legacy `gc_dataset_importer.app` is not changed or called.

## Staging prerequisite

The backend runnables bind the existing staging PostgreSQL resource at
`f/connectors/postgresql` statically, so the browser never receives database
credentials and cannot choose a different database. Use only the sandboxed
staging data when testing Sync.

## Test journey

1. Choose Create, Append, Merge, or Sync.
2. Select or name a target dataset.
3. Upload a synthetic CSV or GeoJSON source. The upload is parsed and staged in
   the POC's private PostgreSQL schema; the target has not changed yet.
4. For Merge and Sync, choose one to three identity fields and an update policy.
5. Generate the SQL-backed preview, then confirm once. Confirmation rejects a
   stale preview and applies all target changes atomically.

The initial POC accepts CSV and GeoJSON only. Use a disposable database: Sync
can delete records that are absent from the staged import.

## Development

Windmill generates `wmill.ts` from `backend/` during `wmill app dev`; it is
included here solely to make the runnable interface explicit in review. Regenerate
it from the staging-compatible CLI before deployment rather than editing it.
