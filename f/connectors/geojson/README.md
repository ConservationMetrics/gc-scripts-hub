# `geojson_to_postgres`: Upload a GeoJSON file to the data warehouse

This script imports data from a GeoJSON file into a database table. It reads a file containing spatial data, transforms the data into a structured format, and inserts it into a PostgreSQL database table. Optionally, it then delete the export file.