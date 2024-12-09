# CoMapeo: Fetch Observations

This script fetches observations and attachments from the REST API of a CoMapeo archive server, which stores data from multiple CoMapeo projects. Each project contains observation data and attachments. The script transforms the data for SQL compatibility and stores it in a PostgreSQL database. Additionally, it downloads any attachments and saves them to a specified local directory.

## Configuration

#### `comapeo_server` (required)

A dictionary containing a server URL and access token key pair to connect to a CoMapeo Archive Server.

#### `attachment_root` (optional, default: "/frizzle-persistent-storage/datalake")

A path where CoMapeo attachments will be stored. Attachment files (e.g., photos and audio) will be stored in the following directory schema: `{attachment_root}/comapeo/my_mapeo_project/attachments/...`

#### `comapeo_project_blocklist` (optional)

An optional blocklist of project IDs to exclude from fetching.

#### `db` (required)

A dictionary containing the database connection parameters for storing tabular data.

#### `db_table_prefix` (optional, default: "comapeo")

This is a prefix added to the database table names created by this script. For each project, the observation data is stored in a table with this prefix. For instance, if `db_table_prefix` is "comapeo" and the project `name` is "My Mapeo Project", the script will create a Postgres table named `comapeo_my_mapeo_project`. If no prefix is provided, the table name will simply be the project `name` without a preceding underscore, such as `my_mapeo_project`.

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