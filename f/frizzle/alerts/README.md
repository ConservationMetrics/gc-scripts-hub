# Google Cloud Alerts Change Detection Integration

This script fetches change detection alerts and images from a storage bucket on Google Cloud Platform. The script transforms the data for SQL compatibility and stores it in a PostgreSQL database. Additionally, it downloads the images and saves them to a specified local directory, and converts several images to JPG.

## Configuration

#### `alerts_bucket` (required)

The name of the Google Cloud Storage bucket where the alerts are stored.

#### `territory_id` (required)

An integer representing the territory ID (areas of interest) for which change detection alerts will be retrieved. This ID is used to filter and process relevant alerts.

#### `gcp_service_acct` (required)

A dictionary containing the Google Cloud service account credentials. This should include the contents of a `service.json` file provided by the alerts publisher. For more information on setting up and retrieving GCP credentials, see: https://cloud.google.com/docs/authentication/provide-credentials-adc#on-prem

#### `db` (required)

A dictionary containing the database connection parameters for storing tabular data.

#### `db_table_name` (required)

The name of the database table where alerts will be stored.

#### `destination_path` (optional)

A string specifying the local directory path where files will be downloaded and processed. Default is `/frizzle-persistent-storage/datalake/change_detection/alerts`.

#### `metadata_filename` (optional)

The filename on GCS to be downloaded by the script during pulls to:

1. Download and produce messages as features per row of the file.
2. Generate a `metadata_uuid` based on the hashed content of the file to ensure a one-time emission of messages for every change to the file.
3. Store each message in the database along with the `metadata_uuid`.

## API Queries

Change Detection alerts can be stored in a Google Cloud storage bucket.

Google Cloud storage has a built-in API solution, [GCS JSON API](https://cloud.google.com/storage/docs/json_api). To access this API regularly, we can use a Google Cloud account service.json key and the `google-cloud-storage` Python library.

## File storage and retrieval

Change detection alert files are currently stored on GCP in this format:

**Vector:**
```
<territory_id>/vector/<year_detec>/<month_detec>/alert_<id>.geojson
```

**Raster:**

```
<territory_id>/raster/<year_detec>/<month_detec>/<sat_detec_prefix>_T0_<id>.tif
<territory_id>/raster/<year_detec>/<month_detec>/<sat_detec_prefix>_T1_<id>.tif
<territory_id>/raster/<year_detec>/<month_detec>/<sat_viz_prefix>_T0_<id>.tif
<territory_id>/raster/<year_detec>/<month_detec>/<sat_viz_prefix>_T1_<id>.tif
```

Currently, we are assuming there to be only four raster files for each change detection alert: a 'before' and 'after' GeoTIFF used for detection and visualization, respectively