# Common logic

This directory contains common logic that is shared between connector scripts.

For more information on sharing common logic in Windmill, see https://www.windmill.dev/docs/advanced/sharing_common_logic

## SQL ingestion

`StructuredDBWriter` remains the backward-compatible connector interface for
dynamic schemas and `_id` upserts. Dataset Importer v2 adds staged Create,
Append, Merge, and Sync workflows without changing that interface.

Both paths share the warehouse contracts in `identifier_utils.py` and
`db_operations.py` for ordered column normalization, persisted `__columns`
mappings, collision handling, and mapping-table names. Import workflows own
their transaction so schema, mapping, and row changes commit or roll back
together.
