# `geojson_to_postgres`: Import a GeoJSON file into a PostgreSQL table

This script reads a GeoJSON file and inserts its contents into a PostgreSQL table, flattening all data into TEXT columns.

### Behavior
 
* Each feature's `geometry` object is decomposed into separate TEXT columns, prefixed with `g__`.
  
  Example: `geometry.type` → `g__type`, `geometry.coordinates` → `g__coordinates`

* Each properties field is inserted as-is into a column matching the property name.
  
  Example: `properties.category` → `category`

* The feature's top-level `id` (if present) is used as the primary key `_id`.

### Notes
* The data is inserted as flat text fields — no geometry types or JSONB columns are used.
* PostGIS is _not_ used at this stage. This approach may change based on requirements downstream.
* Optionally, the input file is deleted after import.
* Currently, this script does not handle GeometryCollection geometries. It will write the GeoJSON to the database, but the `g__coordinates` column will be empty.