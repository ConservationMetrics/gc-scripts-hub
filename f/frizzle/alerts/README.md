# Google Cloud Alerts Change Detection Integration

This script fetches change detection alerts and images from a storage bucket on Google Cloud Platform. The script transforms the data for SQL compatibility and stores it in a PostgreSQL database. Additionally, it downloads the images and saves them to a specified local directory, and converts several images to JPG.

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