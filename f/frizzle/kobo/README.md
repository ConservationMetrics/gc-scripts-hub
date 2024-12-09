# Kobo: Fetch Survey Responses

This script fetches form submissions from the KoboToolbox REST API, transforms the data for SQL compatibility, and stores it in a PostgreSQL database. Additionally, it downloads any attachments and saves them to a specified local directory.

## Configuration

#### `kobotoolbox` (required)

A dictionary containing a server URL and API key pair to connect to the KoboToolbox account.

#### `form_id` (required)

The unique identifier of the form to fetch submissions from.

#### `db` (required)

A dictionary containing the database connection parameters for storing tabular data.

#### `db_table_name` (required)

The name of the database table where the form data will be stored.

#### `attachment_root` (optional, default: "/frizzle-persistent-storage/datalake")

A path where KoboToolbox attachments will be stored. Attachment files will be stored in the following directory schema: `{attachment_root}/{dataset_id}/attachments/{filename}`

## Endpoints

For more information on the KoboToolbox API, see [KoboToolbox API Documentation](https://support.kobotoolbox.org/api.html).