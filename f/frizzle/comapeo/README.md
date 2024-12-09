# CoMapeo: Fetch Observations

This script fetches observations and attachments from the REST API of a [CoMapeo archive server](https://github.com/digidem/comapeo-core/tree/server/src/server), which stores data from multiple CoMapeo projects. Each project contains observation data and attachments. The script transforms the data for SQL compatibility and stores it in a PostgreSQL database. Additionally, it downloads any attachments and saves them to a specified local directory.

## Endpoints

The request header must include an access token in the format: Authorized: Bearer <token>.

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