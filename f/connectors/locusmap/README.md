# Locus Map: Import Data

This script imports data from a Locus Map export file into a database table. It reads a file containing spatial data, transforms the data into a structured format, and inserts it into a PostgreSQL database table. Additionally, it downloads any attachments associated with the spatial data and saves them to a specified directory. Optionally, it then delete the export file.

Locus Map exports data as a CSV, KML, and GPX. If attachments are included, then the export will be as a ZIP file compressing the spatial data file together with a directory containing the attachment files. (It is also possible to export data as a DXF or Ov2 file, but these are not commonly used formats, so this script does not intend to support them.)

The envisioned way to use this script is:

1. User uploads Locus Map data (either as ZIP or CSV/KML/GPX) to the datalake via a web app.
2. Upon completion of the upload, the web app triggers a **[run script by path](https://app.windmill.dev/openapi.html#tag/job/POST/w/{workspace}/jobs/run/p/{path}) Windmill API request** to execute this script as a one-time run. The path of the uploaded file is provided as a parameter.

## Sample curl request for Windmill API

```
curl -X POST \
  https://windmill.demo.guardianconnector.net/api/w/frizzle-demo/jobs/run/p/f/connectors/locusmap/locusmap \
  -H 'Authorization: Bearer Your_Access_Token' \
  -H 'Content-Type: application/json' \
  -d '{
    "db": "$res:f/frizzle/bcmdemo_db",
    "db_table_name": "my_locusmap_points",
    "locusmap_tmp_path": "/frizzle-persistent-storage/tmp/Favorites.zip",
    "attachment_root": "/frizzle-persistent-storage/datalake"
}
# => HTTP 201, no response body
``` 

You can get an Authorization token [via the Windmill CLI](https://www.windmill.dev/docs/advanced/cli/user#creating-a-token), or by finding one in browser request headers once logged in to the Windmill UI.

## TODO

* Support KML, GPX formats in addition to CSV.
* Support tracks in addition to points.