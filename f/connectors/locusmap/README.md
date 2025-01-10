# Locus Map: Import Points

This script imports points from a Locus Map export file into a database table. It reads a file containing point data, transforms the data into a structured format, and inserts it into a PostgreSQL database table. Additionally, it downloads any attachments associated with the points and saves them to a specified directory. It then deletes the temporary files.

Locus Map exports points as a CSV, KML, and GPX. If attachments are included, then the export will be as a ZIP file compressing the point file together with a directory containing the attachment files. (It is also possible to export data as a DXF or Ov2 file, but these are not commonly used formats, so this script does not intend to support them.)

 The envisioned way to use this script is:

1. User uploads Locus Map data (either as ZIP or CSV/KML/GPX) via a web app (`gc-uploads`).
2. Upon completion of the upload, `gc-uploads` triggers a **[run script by path](https://app.windmill.dev/openapi.html#tag/job/POST/w/{workspace}/jobs/run/p/{path}) Windmill API call** to execute this script as a one-time run. The temp path of the uploaded file is provided as a parameter.

> [!IMPORTANT]
> This script assumes that the temporary CSV file and attachments directory should always be deleted after processing.

TODO:

* Use ZIP as input
* Flesh out runtime-example script