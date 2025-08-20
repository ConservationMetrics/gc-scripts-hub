# `csv_to_postgres`: Import a CSV file into a PostgreSQL table

This script reads a CSV file and inserts its contents into a PostgreSQL table, treating all data as TEXT columns.

### Behavior

* Each CSV column is inserted as-is into a database column with the same name.
  
  Example: CSV column `name` → database column `name`

* An `_id` primary key column is automatically added:
  - If `id_column` parameter is specified and exists in the CSV, that column's values are used as `_id` and the original column is removed
  - If no `id_column` is specified, auto-incrementing row numbers (1, 2, 3...) are used as `_id`

* All data is stored as TEXT fields for simplicity and consistency.

### Parameters

* `csv_path`: Path to the CSV file to import
* `db_table_name`: Name of the PostgreSQL table to create/insert into
* `id_column` (optional): Name of existing CSV column to use as primary key
* `delete_csv_file` (optional): Whether to delete the CSV file after import
* `attachment_root` (optional): Root directory where CSV file is located

### Notes

* The CSV file must have headers in the first row
* All values are treated as TEXT regardless of their apparent type
* Optionally, the input file is deleted after import
* If the specified `id_column` doesn't exist in the CSV, auto-incrementing IDs are used instead
