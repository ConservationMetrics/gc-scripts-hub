# Guardian Connector Metrics

This script generates metrics for Guardian Connector services based on provided parameters, and optionally writes them to the `metrics` table in the `guardianconnector` database, with the date recorded as the table's `date` column and its unique identifier (YYYYMMDD format) as the table's `_id` column.

**All parameters are optional**: the script will only collect metrics for services where the required parameters are provided. This allows flexible monitoring of only the services you need.

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

### 7. Windmill
- `number_of_schedules`: Number of scheduled jobs in Windmill

## Auth0 Integration

The Auth0 metrics feature uses the Auth0 Management API to count users. The API endpoint used is:

```
GET https://{auth0_domain}/api/v2/users?search_engine=v3&per_page=1&include_totals=true
```

To use this feature:
1. Create an Auth0 Machine-to-Machine application with access to the Management API
2. Grant the `read:users` scope to the application
3. Set up a Windmill OAuth resource of type `auth0` with the tokenized access
4. Provide the Auth0 domain parameter (e.g., `your-tenant.us.auth0.com`)

The script retrieves only the total count (not the actual user data) for efficiency.