# CoMapeo

This script fetches data from the REST API of a [CoMapeo archive server](https://github.com/digidem/comapeo-core/tree/server/src/server), which stores data from multiple CoMapeo projects. Each project contains observation data and attachments.

The script requires the following configuration values: `comapeo_server_base_url`, `comapeo_access_token`, `attachment_root`, and an optional `comapeo_project_blocklist`. Additionally, the IO manager requires a `db` Postgres resource and `db_table_prefix`.

For each project, the observations data is stored in a table prefixed by `table_prefix`. For example, with a `table_prefix` of "comapeo" and a `name` of "My Mapeo Project", this script will create a Postgres table named `comapeo_my_mapeo_project`. Attachment files (e.g. photos and audio) will be stored in the following directory schema: `{attachment_root}/comapeo/my_mapeo_project/attachments/...`

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