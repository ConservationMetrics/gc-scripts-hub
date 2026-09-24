# GC Dataset Importer v2

React 19 Windmill full-code app for Dataset Importer v2. It runs alongside the
legacy `gc_dataset_importer.app` without changing that app's workflows.

## Import goals

- **Create** creates a dataset from the uploaded records.
- **Append** inserts every uploaded record without matching.
- **Merge** adds unmatched records and retains existing unmatched records.
- **Sync** adds unmatched records and deletes existing unmatched records.
- Merge and Sync use one to three user-selected identity fields and an
  imported-wins or existing-wins update policy.

Every import is staged, reviewed, and confirmed. Confirmation is rejected if
the target or reviewed plan changed. The exact source is archived before any
target writes, and target database changes are transactional. Successful
sources are stored under
`/persistent-storage/datalake/<dataset>`.

## Supported uploads

- CSV, JSON arrays, GeoJSON, GPX, KML, single-layer GeoPackage, XLS, XLSX
- Shapefile ZIPs and ZIPs containing supported files
- CyberTracker backup JSON, detected by content
- SMART patrol XML, detected by namespace

ZIPs are accepted or rejected as a whole. A Create from a multi-file ZIP
appends archive members in order without deduplication. Source uploads are
limited to 25 MiB, expanded archives and spreadsheets to 100 MiB, and final
datasets to 150 columns.

KoboToolbox and ODK source-specific normalization remains in their dedicated
connectors. This app does not ask users to identify a data source.

## Access and resources

The app is private and disallows guests. Windmill app access is the importer
authorization boundary. Backend runnables statically bind
`$res:f/connectors/postgresql`; browser input cannot select database
credentials. Only compatible Guardian Connector dataset tables are listed as
targets.

Use a disposable database when testing Sync. Sync can delete every target row
that is absent from the staged source.

## Development

Run from this directory:

```sh
npm ci
npm run typecheck
npm run format:check
npm run lint
npm test
npm run lint:windmill
npm audit
```

`wmill.ts` documents the generated runnable interface. Windmill supplies the
runtime bridge while linting, bundling, developing, and deploying the raw app.
Regenerate the interface from the target workspace when runnable signatures
change.

## Production rollout

1. Deploy the v2 app with a targeted Windmill deployment before running a full
   repository sync. A full sync will remove repository paths deleted by this
   release and cannot provide a side-by-side transition by itself.
2. Copy and verify app and runnable ACLs in each workspace; a new Windmill path
   does not inherit permissions, bookmarks, schedules, or external callers.
3. Stop the previous preview app, back up PostgreSQL, and set
   `DATASET_IMPORTER_V2_MIGRATE_SCHEMA` to its importer schema name before the
   first v2 request. Schema adoption preserves staged sessions but is one-way;
   rollback after adoption requires restoring the database backup. Remove the
   migration variable after the first successful v2 request.
4. Update links and integrations to the v2 path. Do not run the previous preview
   app after v2 has adopted its schema.
5. Verify Create, Append, Merge, and Sync against disposable copies of mapped,
   unmapped, tabular, spatial, CyberTracker, SMART, and archive datasets.
6. Compare preview counts with committed rows, mappings, geometry, and datalake
   artifacts.
7. Confirm Windmill permissions, PostgreSQL privileges, persistent storage,
   cleanup behavior, logs, and rollback access.
8. Only then run the full repository sync that removes the previous preview app;
   keep the legacy Dataset Importer v1 until its own consumers are migrated.

If archival fails, no target changes are made and the reviewed session retains
the source until expiry so the same confirmation can be retried. Failed or
abandoned staged sources are removed after expiry on the next importer app load
or backend operation. A deployment may also schedule `cleanup_expired_imports`
when cleanup must run without importer activity.
