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
