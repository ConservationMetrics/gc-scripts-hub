# `comapeo_pull`: Fetch Observations and Tracks from CoMapeo API

This script fetches observations and tracks from the REST API of a [CoMapeo archive server](https://github.com/digidem/comapeo-core/tree/server/src/server), which stores data from multiple CoMapeo projects. Each project contains observation data, track data, and attachments.

For each project, the observations and tracks data are stored in separate tables prefixed by `table_prefix` with `_observations` and `_tracks` suffixes. 

For example, with a `table_prefix` of "comapeo" and a `name` of "My Mapeo Project", this script will create Postgres tables named `comapeo_my_mapeo_project_observations` and `comapeo_my_mapeo_project_tracks`. 

**Attachments:** media attachment files (e.g. photos and audio) will be stored in the following directory schema: `{attachment_root}/comapeo/{project_name}/attachments/...`

Tracks do not have attachments, so no attachment downloading is performed for tracks.

**Icons:** icons for each preset will be stored in the following directory schema: `{attachment_root}/comapeo/{project_name}/icons/...`

## Endpoints

The request header must include an access token in the format: 

    Authorized: Bearer <token>.

See the fixture data in the [server_responses.py](./tests/assets/server_responses.py) file for complete responses from the server for the endpoints below.

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

### `GET /projects/abc123/observation`

```json
{
  "docId": "987xyz",
  ...
  "attachments": [
    { "url": "https://comapeo.example/projects/abc123/attachments/attachment1_hash/photo/blob1_hash" },
    { "url": "https://comapeo.example/projects/abc123/attachments/attachment2_hash/photo/blob2_hash" }
  ],
  "tags": {
    ...
  },
  "lat": -12,
  "lon": 34,
  "metadata": {
    ...
  }.
  "presetRef": {
    "docId": "preset_doc_id",
    "versionId": "version_id",
    "url": "https://comapeo.example/projects/abc123/preset/preset_doc_id"
  }
}
```

### `GET /projects/abc123/track`

```json
{
  "docId": "track_xyz",
  ...
  "locations": [
    {
      "timestamp": "2024-10-09T08:07:06.543Z",
      "mocked": false,
      "coords": {
        "latitude": -12,
        "longitude": 34
      }
    },
    ...
  ],
  "observationRefs": [],
  "tags": {},
  "presetRef": {
    "docId": "preset_doc_id",
    "versionId": "version_id",
    "url": "https://comapeo.example/projects/abc123/preset/preset_doc_id"
  }
}
```

Tracks are stored with LineString geometry (from the `locations` array coordinates) and a parallel `timestamps` array in the database. The timestamps array corresponds to each coordinate point in the LineString, with index 0 in both arrays aligning.

### `GET /projects/abc123/attachments/:attachmentHash/photo/:docId`

This endpoint retrieves the binary data of a specific attachment, such as a photo, associated with a project. The response will contain the raw binary content of the file, which can be saved or processed as needed.

### `GET /projects/abc123/preset/:docId`

```json
{
  "docId": "preset_doc_id",
  ...
  "name": "My Preset",
  ...
  "terms": ["term1", "term2"],
  "iconRef": {
    "docId": "icon_doc_id",
    "url": "https://comapeo.example/projects/abc123/icon/icon_doc_id"
  }
  "color": "#000000"
}
```

This endpoint retrieves the preset data for a given preset `docId`. Per the [CoMapeo schema documentation](https://github.com/digidem/comapeo-schema/blob/main/schema/preset/v1.json), presets define how map entities are displayed to the user. They define the category that is assigned to an observation, the icon used on the map, and the fields / questions shown to the user when they create or edit the entity on the map. 

Currently, we use this endpoint in the Fetch Observations and Tracks script to fetch **some** of the preset data for a given observation or track that we think our users are most interested in having available in properties.

### `GET /projects/abc123/icon/:docId`

This endpoint retrieves the binary data of a specific icon file associated with a preset. The response will contain the raw binary content of the file (typically a PNG image).

# `comapeo_alerts`: Post Alerts to CoMapeo API

This script fetches alerts data from a database and posts it to a CoMapeo server.

## Endpoints

The request header must include an access token in the format: 

    Authorized: Bearer <token>.

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