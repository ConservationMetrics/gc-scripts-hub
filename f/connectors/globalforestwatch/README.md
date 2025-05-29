# `gfw_alerts`: Download and Store Alerts from Global Forest Watch

This script can retrieve various types of alerts, such as GLAD and Integrated Forest Alerts, from the [Global Forest Watch Data API](https://data-api.globalforestwatch.org/), for a given bounding box. The alerts are then formatted as GeoJSON and stored in a PostgreSQL database. The script also saves the GeoJSON files to a specified directory for further use.

Currently, we support fetching the following alerts from GFW:

* [Integrated deforestation alerts](https://data.globalforestwatch.org/datasets/gfw::integrated-deforestation-alerts/about)
* [GLAD alerts](https://glad.umd.edu/dataset/glad-forest-alerts) - either Landsat or Sentinel-2
* [RADD alerts](https://data.globalforestwatch.org/datasets/gfw::deforestation-alerts-radd/about)
* [NASA VIIRS fire alerts](https://data.globalforestwatch.org/documents/gfw::viirs-active-fires/about)

> [!NOTE]
> This script makes a query request to conduct on-the-fly data analysis. This means that for large areas, it may take a while for the GFW API to return the result. Additionally, there is a **maximum allowed payload size of 6291556 bytes**.
>
> If we ever need to work around this payload size limit, we can use a [batch download endpoint](https://data-api.globalforestwatch.org/#tag/Query/operation/query_dataset_list_post_dataset__dataset___version__query_batch_post). This endpoint will provide a job ID, and trigger the query in the background. Once complete, accessing the job ID via a ` /job/{job_id}` endpoint will return a download link.

To use this script, **you need a valid API key from the GFW Data API**: See [Getting a GFW API Key](#getting-a-gfw-api-key) below.

## Getting a GFW API Key

These are the steps for getting a Global Forest Watch Data API key.

### 1. Sign up for a Resource Watch API account

https://api.resourcewatch.org/

### 2. Get a JWT token

https://resource-watch.github.io/doc-api/quickstart.html#2-obtain-your-jwt-for-authentication

This token will not expire unless you change your user information.

### 3. Get the GFW API key

https://resource-watch.github.io/doc-api/quickstart.html#3-create-your-application-and-get-your-api-key

```bash
curl -X POST "https://data-api.globalforestwatch.org/auth/apikey" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <your-jwt-token>" \
  -d '{
    "alias": "test-key",
    "email": "your-email@domain.com",
    "organization": "Your Organization Name",
    "domains": []}'
```

> [!NOTE] 
> The API key expires after one year. You will need to retrieve a new key afterwards. In the future, we can look at doing this programmatically.