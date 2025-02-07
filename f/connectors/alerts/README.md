This README introduces two scripts that live in this folder:

- `alerts_gcs`: Google Cloud Alerts Change Detection Integration
- `alerts_twilio`: Send a Twilio Message

# `alerts_gcs`: Google Cloud Alerts Change Detection Integration

This script fetches change detection alerts and images from a storage bucket on Google Cloud Platform. The script transforms the data for SQL compatibility and stores it in a PostgreSQL database. Additionally, it saves before-and-after images -- as TIF and JPEG -- to a specified directory.

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

# `alerts_twilio`: Send a Twilio Message 

This script leverages Twilio to send a WhatsApp message to recipients with a summary of the latest processed alerts. Below is the message template, with values from an `alerts_statistics` object:

```javascript
`${total_alerts} new change detection alert(s) have been published on your alerts dashboard for the date of ${month_year}. The following activities have been detected in your region: ${description_alerts}. Visit your alerts dashboard here: https://explorer.${community_slug}.guardianconnector.net/alerts/alerts. If you are using CoMapeo with an archive server enabled, you can receive the alerts by synchronizing.`
```

There are translations in English, Dutch, Portuguese, and Spanish available, with a unique content template [SID](https://www.twilio.com/docs/glossary/what-is-a-sid) for each.