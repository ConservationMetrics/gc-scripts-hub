# Kobo: Fetch Survey Responses

Uses KoboToolbox REST API to download all form submissions.

## Configuration

#### `kobotoolbox` (required)

A server and API key pair to connect to the KoboToolbox form.

#### `form_id` (required)

The unique identifier of the form to fetch submissions from.

#### `db` (required)

A PostgreSQL database connection.

#### `db_table_name` (required)

The name of the database table where the form data will be stored.

#### `attachment_root` (optional, default: "/frizzle-persistent-storage/datalake")

A path where KoboToolbox attachments will be stored. Attachment files will be stored in the following directory schema: `{attachment_root}/{dataset_id}/attachments/{filename}`

## Endpoints

For more information on the KoboToolbox API, see [KoboToolbox API Documentation](https://support.kobotoolbox.org/api.html).