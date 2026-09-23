# GC Dataset Importer v2 Pilot

React 19 Windmill full-code app for the Dataset Importer v2 pilot. It runs
alongside `gc_dataset_importer.app` until production validation and cutover are
complete.

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

## Pilot and cutover

1. Deploy under the v2 pilot path without replacing the legacy importer.
2. Verify Create, Append, Merge, and Sync against disposable copies of mapped,
   unmapped, tabular, spatial, CyberTracker, SMART, and archive datasets.
3. Compare preview counts with committed rows, mappings, geometry, and datalake
   artifacts.
4. Confirm Windmill permissions, PostgreSQL privileges, persistent storage,
   cleanup behavior, logs, and rollback access.
5. Publish the v2 path after pilot acceptance, then retire the legacy app only
   after remaining workflows have moved to v2 or their dedicated connectors.

If archival fails, no target changes are made and the reviewed session retains
the source until expiry so the same confirmation can be retried. Failed or
abandoned staged sources are removed when their sessions expire.
