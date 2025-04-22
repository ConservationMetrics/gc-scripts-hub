# `gfw_alerts`: Download and Store Alerts from Global Forest Watch

This script can retrieve various types of alerts, such as GLAD and Integrated Forest Alerts, from the [Global Forest Watch Data API](https://data-api.globalforestwatch.org/), for a given bounding box. The alerts are then formatted as GeoJSON and stored in a PostgreSQL database. The script also saves the GeoJSON files to a specified directory for further use.

Currently, we support fetching the following alerts from GFW:

* [Integrated deforestation alerts](https://data.globalforestwatch.org/datasets/gfw::integrated-deforestation-alerts/about)
* [GLAD alerts](https://glad.umd.edu/dataset/glad-forest-alerts) - either Landsat or Sentinel-2
* [RADD alerts](https://data.globalforestwatch.org/datasets/gfw::deforestation-alerts-radd/about)

To use this script, **you need a valid API key from the GFW Data API**: See [Getting a GFW API Key](#getting-a-gfw-api-key) below.

> [!NOTE]
> This script makes a query request to conduct on-the-fly data analysis. This means that for large areas, it may take a while for the GFW API to return the result. Additionally, there is a maximum allowed payload size of 6291556 bytes.

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

## Future extensions

### Utilizing a MyGFW area and the GFW Geostore API

It may be possible to use the [GFW Geostore API](https://data-api.globalforestwatch.org/#tag/Geostore) to query and retrieve alerts for a [saved area created in MyGFW](https://www.globalforestwatch.org/help/map/guides/manage-saved-areas/).

You can retrieve a Geostore ID from the Resource Watch API by [requesting all user areas](https://resource-watch.github.io/doc-api/reference.html#getting-all-user-areas).

Pros and cons:

* Pros in favor of doing this include not needing to input a bounding box in Windmill, and potentially leveraging pre-calculated data. 
* One con is that this approach requires managing resources on MyGFW, whereas the `gfw_alerts` script currently does not require using Global Forest Watch at all (except to get an API key).

#### 1. Providing the `geostore_id` as a query parameter

This approach obviates the need to provide a bounding box directly:

```python
url = (
    "https://data-api.globalforestwatch.org/dataset/gfw_integrated_alerts/latest/query"
)
headers = {
    "x-api-key": "<your_api_key>",
    "Content-Type": "application/json",
}
params = {
    "sql": "SELECT latitude, longitude, gfw_integrated_alerts__date, gfw_integrated_alerts__confidence FROM results WHERE gfw_integrated_alerts__date >= '2025-01-01'",
    "geostore_origin": "rw",
    "geostore_id": "<your_geostore_id>",
}

response = requests.get(url, headers=headers, params=params)
```

#### 2. Leveraging pre-calculated `geostore__` endpoints?

Perhaps most promising of all, it may be possible to use special endpoints with a `geostore__` prefix that provide pre-calculated data. For example:

```python
url = "https://data-api.globalforestwatch.org/dataset/geostore__integrated_alerts__daily_alerts/latest/query"
headers = {
    "x-api-key": "<your_api_key>",
    "Content-Type": "application/json",
}
params = {
    "sql": "SELECT gfw_integrated_alerts__date, gfw_integrated_alerts__confidence FROM results WHERE gfw_integrated_alerts__date >= '2025-01-01'",
    "geostore_origin": "rw",
    "geostore_id": "<your_geostore_id>",
}

response = requests.get(url, headers=headers, params=params)
 ```

 However, while this is a valid request, this endpoint seems to not return latitude or longitude, so this approach requires more research.