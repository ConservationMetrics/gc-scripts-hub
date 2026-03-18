# GC Local Contexts Annotations Application

This Windmill app applies Local Contexts labels to an existing dataset by writing a
small per-dataset mapping table.

## Prerequisite

Before using this app, a Local Contexts label-set must already exist in Postgres.
In practice, this means running `f/connectors/localcontexts/localcontexts_pull.py`
first, which creates a label table like:

- `localcontexts_<project_title_normalized>`

That table is then selected in this app as the source of available TK/BC labels.

See the [Local Contexts: Pull Labels](../../connectors/localcontexts/README.md) documentation for more details.

## What this app does, code-wise

1. Lists available dataset tables in Postgres (`1_fetch_tables_from_postgres.inline_script.py`).
2. Reads available labels from the selected Local Contexts label-set table and splits
   them into TK and BC options (`1_fetch_lc_labels_and_existing_mappings.inline_script.py`). 
3. The same Python inline script reads any labels already applied to the selected dataset from
   `{dataset_name}__lc_labels` and returns them as:
   - `tk_labels_already_applied`
   - `bc_labels_already_applied`
4. Writes the updated mapping to `{dataset_name}__lc_labels`
   (`3_write_label_mapping_to_database_table.inline_script.py`):
   - ensures the table exists with `_id` and `label`
   - truncates existing rows
   - inserts the newly selected labels

## Notes

- This app updates label mappings only. It does not modify rows in the main dataset table.
- Reapplying labels overwrites prior mapping state for that dataset (truncate + insert).