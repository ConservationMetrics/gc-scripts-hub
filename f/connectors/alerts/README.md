# `alerts_gcs`: Google Cloud Alerts Change Detection Integration

This script fetches change detection alerts and images from a storage bucket on Google Cloud Platform. The script transforms the data for SQL compatibility and stores it in a PostgreSQL database. Additionally, it saves before-and-after images -- as TIF and JPEG -- to a specified directory.

**Dataset** is `use_existing_dataset` or `create_new_dataset` (default). The form shows these as "Use existing dataset" and "Enter dataset name". Entering a name creates the dataset if needed and updates it if it already exists. Existing datasets are chosen from the public tables on `db`. A new dataset name is at most 53 characters so `{name}__metadata` still fits.

**Note:** This script does not support GeoJSON files containing GeometryCollection geometries and will fail if any are detected.

## GCP API Queries

Change Detection alerts can be stored in a Google Cloud storage bucket.

Google Cloud storage has a built-in API solution, [GCS JSON API](https://cloud.google.com/storage/docs/json_api). To access this API regularly, we can use a Google Cloud account service.json key and the `google-cloud-storage` Python library.

## File storage and retrieval

### Google Cloud Platform

Change detection alert files are currently stored on GCP in this format:

**Vector:**

    <territory_id>/vector/<year_detec>/<month_detec>/alert_<id>.geojson


**Raster:**


    <territory_id>/raster/<year_detec>/<month_detec>/<sat_detec_prefix>_T0_<id>.tif
    <territory_id>/raster/<year_detec>/<month_detec>/<sat_detec_prefix>_T1_<id>.tif
    <territory_id>/raster/<year_detec>/<month_detec>/<sat_viz_prefix>_T0_<id>.tif
    <territory_id>/raster/<year_detec>/<month_detec>/<sat_viz_prefix>_T1_<id>.tif

### Warehouse

**Vector:**

    <territory_id>/<year_detec>/<month_detec>/<alert_id>/alert_<id>.geojson

**Raster:**
Currently, we are assuming there to be only four raster images for each change detection alert: a 'before' and 'after' used for detection and visualization, respectively.  Each of these is saved in both TIFF and JPEG format in the following way:

    <territory_id>/<year_detec>/<month_detec>/<alert_id>/images/<sat_viz_prefix>_T0_<id>.tif
    <territory_id>/<year_detec>/<month_detec>/<alert_id>/images/<sat_viz_prefix>_T0_<id>.jpg
    ...

WhatsApp notification after new alerts are written is handled by [`twilio_change_detection`](../twilio/README.md). The [Alerts: Download, Post, and Notify](../../flows/alerts_download_post_notify.flow/README.md) flow runs that script after this one.