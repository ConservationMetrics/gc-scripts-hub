# Guardian Connector Metrics

This script generates comprehensive metrics for Guardian Connector services based on provided parameters.

**All parameters are optional** - the script will only collect and return metrics for services where the required parameters are provided. This allows flexible monitoring of only the services you need.

## Metrics Collected

### 1. CoMapeo
- `project_count`: Number of projects on the CoMapeo server
- `data_size_mb`: Size of pulled CoMapeo data on the datalake (in MB)

> [!NOTE]
> 
> The data size measurement looks at the `{attachment_root}/comapeo` directory on the datalake, not the CoMapeo server's Docker volume, as Windmill doesn't have mounted access to Docker volumes. This serves as a proxy metric based on the pulled data stored locally.

### 2. Warehouse
- `total_tables`: Total number of tables in the data warehouse (public schema)
- `total_records`: Total number of records across all warehouse tables

### 3. Explorer
- `dataset_views`: Number of dataset views configured in the guardianconnector database (`view_config` table)

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

## Output Format

The script returns a nested JSON structure with metrics organized by service:

```json
{
  "comapeo": {
    "project_count": 3,
    "data_size_mb": 3379.37
  },
  "warehouse": {
    "total_tables": 50,
    "total_records": 1000000
  },
  "explorer": {
    "dataset_views": 11
  },
  "superset": {
    "dashboards": 5,
    "charts": 25
  },
  "files": {
    "file_count": 5000,
    "data_size_mb": 10000.50
  },
  "auth0": {
    "users": 150
  },
  "windmill": {
    "number_of_schedules": 15
  }
}
```

## Parameters

All parameters are optional. The script will only collect metrics for services where the required parameters are provided.

### CoMapeo Metrics
- **comapeo** (object, optional): CoMapeo server connection details
  - `server_url`: URL of the CoMapeo server
  - `access_token`: Authentication token for the CoMapeo API
  - If not provided: CoMapeo metrics will be skipped

### Database Metrics (Warehouse, Explorer, Superset)
- **db** (object, optional): PostgreSQL connection parameters used for all database metrics
  - The script will use this connection and switch to different databases as needed
  - If not provided: All database metrics (warehouse, Explorer, Superset) will be skipped
- **guardianconnector_db** (string, optional): Database name for Explorer metrics (default: `guardianconnector`)
- **superset_db** (string, optional): Database name for Superset metrics (default: `superset_metastore`)

### Files Metrics
- **attachment_root** (string, optional): Path to the datalake root directory (default: `/persistent-storage/datalake`)
  - Used for CoMapeo data size and files metrics
  - Always attempts to collect metrics using the default path

### Auth0 Metrics
- **auth0_resource** (oauth resource, optional): OAuth resource with tokenized access to Auth0 Management API
  - If not provided: Auth0 metrics will be skipped
- **auth0_domain** (string, optional): The Auth0 domain (e.g., `your-tenant.us.auth0.com`)
  - If not provided: Auth0 metrics will be skipped
  - Both `auth0_resource` and `auth0_domain` must be provided to collect Auth0 metrics

### Windmill Metrics
Windmill metrics are **automatically collected** when the script runs inside a Windmill worker. No parameters needed!

The script uses these environment variables that Windmill automatically injects:
- `WM_TOKEN`: Authentication token
- `WM_BASE_URL`: Base URL of the Windmill instance  
- `WM_WORKSPACE`: Current workspace

If these environment variables are not present (e.g., running outside Windmill), Windmill metrics will be skipped.

## Usage

This script is designed to be run as a scheduled job in Windmill. The typical workflow is:

1. Provide only the resources/parameters for services you want to monitor
2. The script will collect metrics for those services and skip others
3. Schedule the script to run periodically (e.g., daily, hourly)
4. Use the output metrics for monitoring dashboards, alerts, or reporting

### Example Use Cases

**Monitor only files:**
```python
# When running outside Windmill, only files metrics collected
result = main()
# Returns: {"files": {...}}
```

**Monitor only database services:**
```python
# Provide only db, skip comapeo
result = main(db={"host": "localhost", ...})
# Returns: {"warehouse": {...}, "explorer": {...}, "superset": {...}, "files": {...}}
# Plus "windmill": {...} if running inside Windmill
```

**Monitor everything:**
```python
# Provide all optional parameters
result = main(
    comapeo={"server_url": "...", "access_token": "..."},
    db={"host": "localhost", ...},
    auth0_resource={"token": "..."},
    auth0_domain="your-tenant.us.auth0.com"
)
# Returns: {"comapeo": {...}, "warehouse": {...}, "explorer": {...}, "superset": {...}, "files": {...}, "auth0": {...}, "windmill": {...}}
```

## Windmill Self-Monitoring

The Windmill metrics feature leverages environment variables that Windmill automatically injects into every job:

- **`WM_TOKEN`**: Authentication token for the Windmill API
- **`WM_BASE_URL`**: Base URL of the Windmill instance  
- **`WM_WORKSPACE`**: Current workspace name

The script checks for these variables and, if present, calls the Windmill API endpoint:

```
GET {WM_BASE_URL}/api/w/{WM_WORKSPACE}/schedules/list
```

This allows the script to report on itself and the Windmill instance it's running in, without requiring any manual configuration!

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

## Error Handling