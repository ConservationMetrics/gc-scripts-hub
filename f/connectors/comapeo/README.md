# `comapeo_observations`: Fetch Observations from CoMapeo API

This script fetches data from the REST API of a [CoMapeo archive server](https://github.com/digidem/comapeo-core/tree/server/src/server), which stores data from multiple CoMapeo projects. Each project contains observation data and attachments.

For each project, the observations data is stored in a table prefixed by `table_prefix`. For example, with a `table_prefix` of "comapeo" and a `name` of "My Mapeo Project", this script will create a Postgres table named `comapeo_my_mapeo_project`. Attachment files (e.g. photos and audio) will be stored in the following directory schema: `{attachment_root}/comapeo/my_mapeo_project/attachments/...`

## Endpoints

The request header must include an access token in the format: 

    Authorized: Bearer <token>.

### `GET /projects`

```json
{
  "data": [
    {
      "projectId": "abc123",
      "name": "My Mapeo Project"
    }
  ]
}
```

### `GET /projects/abc123/observations`

```json
{
  "docId": "987xyz",
  "createdAt": "2024-10-09T08:07:06.543.Z",
  "updatedAt": "2024-10-09T08:07:06.543.Z",
  "deleted": false,
  "lat": -12,
  "lon": 34,
  "attachments": [
    { "url": "https://comapeo.example/projects/abc123/attachments/attachment1_hash/photo/blob1_hash" },
    { "url": "https://comapeo.example/projects/abc123/attachments/attachment2_hash/photo/blob2_hash" }
  ],
  "tags": {}
}
```

### `GET /projects/abc123/attachments/attachment2_hash/photo/blob2_hash`

This endpoint retrieves the binary data of a specific attachment, such as a photo, associated with a project. The response will contain the raw binary content of the file, which can be saved or processed as needed.

# `comapeo_alerts`: Post Alerts to CoMapeo API

This script fetches alerts data from a database and posts it to a CoMapeo server.

## Endpoints

The request header must include an access token in the format: Authorized: Bearer <token>.

### `POST /projects/abc123/remoteDetectionAlerts`

```json
{
  "detectionDateStart": "2024-11-03T04:20:69Z",
  "detectionDateEnd": "2024-11-04T04:20:69Z",
  "sourceId": "abc123",
  "metadata": { "foo": "bar" },
  "geometry": {
    "type": "Point",
    "coordinates": [12, 34]
  }
}
# => HTTP 201, no response body
```

### `GET /projects/abc123/remoteDetectionAlerts`

```json
[
  {
    "detectionDateStart": "2024-11-03T04:20:69Z",
    "detectionDateEnd": "2024-11-04T04:20:69Z",
    "sourceId": "abc123",
    "metadata": { "foo": "bar" },
    "geometry": {
      "type": "Point",
      "coordinates": [12, 34]
    }
  },
  ...
]
```