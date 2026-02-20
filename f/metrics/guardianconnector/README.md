# Guardian Connector Metrics

This script generates metrics for Guardian Connector services based on provided parameters, and optionally writes them to the `metrics` table in the `guardianconnector` database, with the date recorded as the table's `date` column and its unique identifier (YYYYMMDD format) as the table's `_id` column.

## Notes

- All parameters are optional: the script will only collect metrics for services where the required parameters are provided. This allows flexible monitoring of only the services you need.

- This script will create the `guardianconnector` database if it doesn't exist. This is done in case the database is not already created by another resource on the stack, like GuardianConnector Explorer.

- The intended usage of this script is to be scheduled to run once a month, so it can be used to monitor and collect metrics data about usage over time.

## Metrics Collected

### 1. CoMapeo
- `project_count`: Number of projects on the CoMapeo server
- `data_size_mb`: Size of pulled CoMapeo data on the datalake (in MB)

> [!NOTE]
> 
> The data size measurement looks at the `{attachment_root}/comapeo` directory on the datalake, **not the CoMapeo server's Docker volume**, as Windmill doesn't have mounted access to Docker volumes. This serves as a proxy metric based on the pulled data stored locally, and therefore assumes that a [`comapeo_pull`](../../connectors/comapeo/README.md) script has been run to pull the CoMapeo project data to the datalake.

### 2. Warehouse
- `total_tables`: Total number of tables in the data warehouse (public schema)
- `total_records`: Total number of records across all warehouse tables

### 3. Explorer
- `dataset_views`: Number of dataset views configured

### 4. Superset
- `dashboards`: Number of dashboards in Superset
- `charts`: Number of charts (slices) in Superset

### 5. Files
- `file_count`: Total number of files in the datalake
- `data_size_mb`: Total size of all files in the datalake (in MB)

### 6. Auth0
- `users`: Total number of users in Auth0
- `users_signed_in_past_30_days`: Number of users who signed in during the past 30 days
- `logins`: Total number of logins across all users (lifetime)

### 7. Windmill
- `schedules`: Number of scheduled jobs in Windmill

## Auth0 Integration

The Auth0 metrics feature uses the Auth0 Management API to collect user and session statistics. 

To use this feature:
1. Create an Auth0 Machine-to-Machine (M2M) application in your Auth0 dashboard
2. Authorize it for the Auth0 Management API
3. Grant the `read:users` and `read:stats` scopes to the application
4. Create a Windmill resource of type `oauth_application` with:
   - `client_id`: Your M2M application's client ID
   - `client_secret`: Your M2M application's client secret
   - `domain`: Your Auth0 domain (e.g., `your-tenant.us.auth0.com`)

The script uses the OAuth 2.0 client credentials flow to obtain an access token, then queries:
- `/api/v2/users` - Total user count
- `/api/v2/users?q=last_login:[...]` - Active users in past 30 days
- `/api/v2/users?fields=logins_count` - Paginated query to sum all users' login counts

## TODO

- [ ] Document need for a M2M application in Auth0 in `gc-deploy`
- [ ] Roll out to all instances (schedule to run once a month)