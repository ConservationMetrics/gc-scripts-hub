# GC Dataset Importer v2

React 19 Windmill full-code app for Dataset Importer v2.

## Goals
Support more complex 'import strategies', where v1 only supported basic merges.
- **Create** creates a dataset from the uploaded records.
- **Append** inserts every uploaded record without matching.
- **Merge** adds unmatched records and retains existing unmatched records.
- **Sync** adds unmatched records and deletes existing unmatched records.

Merge and Sync are possible because we enable the user to select up to three columns to use as "identity" as well as a conflict policy (imported wins, or existing wins).

Every selected identity field must be present and populated in every uploaded
record. Missing fields, nulls, empty strings, and whitespace-only strings are
rejected during preview; the combined identity must be unique in the upload.
Zero, false, and literal strings such as "null" and "undefined" are valid.
Multiple target rows matching an uploaded identity are rejected under either
policy, even if their other values are identical. Incomplete target identities
never match. Merge can retain existing duplicates and incomplete identities;
Sync deletes **every unmatched row in the entire selected table**, including
records from other sources, even when Existing records win is selected.

The identity dropdown and review show the actual source-to-stored field mapping.
Uploaded `_id` normally maps to `source_id`; it does not select the target's
internal Postgres `_id`. Colliding source fields keep distinct stored names.
Imports do not repair missing identities, backfill source IDs, or deduplicate
retained rows. Empty ordinary fields remain valid and follow the update policy;
omitted fields on matching rows are preserved.

Pending previews from older identity rules must be reviewed again. Successful
confirmations remain idempotent.

Complete feature spec with acceptance criteria was written to [./SPEC.md](./SPEC.md)

Successful sources are stored under
`/persistent-storage/datalake/<dataset>`.

## Editing app text

UI labels, instructions, and fallback errors live in [`strings.json`](./strings.json).
Keep named placeholders such as `{dataset}` and `{count}` when editing messages.
Singular and plural messages have separate entries. Backend validation messages
are displayed as returned by the importer.

## Supported uploads

- CSV, JSON arrays, GeoJSON, GPX, KML, single-layer GeoPackage, XLS, XLSX
- Shapefile ZIPs and ZIPs containing supported files
- CyberTracker backup JSON, detected by content
- SMART patrol XML, detected by namespace

## Known Limitations

- No "replace dataset" option. Decided against it just for simplicity. The difference between "sync" and "replace" is that "sync" retains existing columns even if they don't exist in the newly imported dataset where "replace" would not have.
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
2. **Preview:** PostgreSQL checks uploaded identities for completeness and uniqueness
   and rejects multiple target matches for an uploaded identity
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

## Generated app bindings

Run `npx wmill app dev --no-open` from this app directory after changing backend
runnables; stop the server once it generates `wmill.d.ts`, then run
`mv wmill.d.ts wmill.ts` to retain the module path used by Vitest, followed by
`npx prettier --write wmill.ts`.
The CLI generates runnable argument declarations;
Python response shapes are represented by the app's local TypeScript types,
including `StagedImport.source_mapping`. Do not edit the generated declarations
by hand. Run `npm run typecheck`, `npm run lint`, and `npm test` after generation.
