# requirements:
# requests~=2.32
# psycopg

import logging
import os
import subprocess
from pathlib import Path
from typing import Optional, TypedDict

import psycopg
import requests

from f.common_logic.db_operations import conninfo, postgresql


class comapeo_server(TypedDict):
    server_url: str
    access_token: str


class auth0(TypedDict):
    token: str


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_directory_size(directory_path: str) -> Optional[int]:
    """Get the size of a directory in bytes.

    Parameters
    ----------
    directory_path : str
        Path to the directory.

    Returns
    -------
    Optional[int]
        Size in bytes, or None if the path doesn't exist or access fails.
    """
    path = Path(directory_path)

    if not path.exists():
        logger.warning(f"Directory path does not exist: {directory_path}")
        return None

    try:
        result = subprocess.run(
            ["du", "-sb", str(path)],
            capture_output=True,
            text=True,
            check=True,
        )
        size_bytes = int(result.stdout.split()[0])
        return size_bytes
    except (subprocess.CalledProcessError, ValueError, IndexError, Exception) as e:
        logger.error(f"Failed to get directory size: {e}")
        return None


def get_comapeo_metrics(
    comapeo: comapeo_server,
    attachment_root: str = "/persistent-storage/datalake",
) -> dict:
    """Get metrics for CoMapeo server.

    Parameters
    ----------
    comapeo : comapeo_server
        Dictionary containing 'server_url' and 'access_token' for the CoMapeo server.
    attachment_root : str, optional
        Path to the datalake root directory where CoMapeo data is stored.
        Defaults to "/persistent-storage/datalake".

    Returns
    -------
    dict
        A dictionary containing metrics: project_count, data_size_mb.
    """
    server_url = comapeo["server_url"]
    access_token = comapeo["access_token"]

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {access_token}"})

    url = f"{server_url}/projects"
    logger.info("Fetching projects from CoMapeo API...")

    response = session.get(url)
    response.raise_for_status()

    projects = response.json().get("data", [])
    project_count = len(projects)

    logger.info(f"Total number of projects on CoMapeo server: {project_count}")

    # Get size of comapeo data directory
    comapeo_data_path = Path(attachment_root) / "comapeo"
    data_size_bytes = get_directory_size(str(comapeo_data_path))

    metrics = {"project_count": project_count}

    if data_size_bytes is not None:
        data_size_mb = round(data_size_bytes / (1024**2), 2)
        metrics["data_size_mb"] = data_size_mb
        logger.info(
            f"CoMapeo data directory size (on datalake/comapeo dir, not the CoMapeo volume): {data_size_mb} MB"
        )
    else:
        logger.warning("Could not determine data directory size")

    return metrics


def get_warehouse_metrics(db: postgresql) -> dict:
    """Get metrics for the data warehouse.

    Parameters
    ----------
    db : postgresql
        Database connection parameters for the warehouse.

    Returns
    -------
    dict
        A dictionary containing metrics: total_tables, total_records.
    """
    logger.info("Fetching warehouse metrics...")

    try:
        conn_str = conninfo(db)
        with psycopg.connect(conn_str, autocommit=True) as conn:
            with conn.cursor() as cursor:
                # Count total tables in public schema
                cursor.execute("""
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_type = 'BASE TABLE'
                """)
                table_count = cursor.fetchone()[0]

                # Get total records by summing COUNT(*) from each table
                cursor.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_type = 'BASE TABLE'
                """)
                table_names = cursor.fetchall()

                records = 0
                for (table_name,) in table_names:
                    try:
                        cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
                        count = cursor.fetchone()[0]
                        records += count
                    except Exception as e:
                        logger.warning(
                            f"Could not count records in table {table_name}: {e}"
                        )

        logger.info(f"Warehouse: {table_count} tables, {records:,} total records")

        return {
            "tables": table_count,
            "records": records,
        }
    except Exception as e:
        logger.error(f"Failed to fetch warehouse metrics: {e}")
        return {}


def get_explorer_metrics(db: postgresql) -> dict:
    """Get metrics for Explorer (guardianconnector database).

    Parameters
    ----------
    db : postgresql
        Database connection parameters for the guardianconnector database.

    Returns
    -------
    dict
        A dictionary containing metrics: dataset_views (count of records in view_config table).
    """
    logger.info("Fetching Explorer metrics...")

    try:
        conn_str = conninfo(db)
        with psycopg.connect(conn_str, autocommit=True) as conn:
            with conn.cursor() as cursor:
                # Count records in view_config table
                cursor.execute("""
                    SELECT COUNT(*)
                    FROM view_config
                """)
                dataset_views = cursor.fetchone()[0]

        logger.info(f"Explorer: {dataset_views} dataset views")

        return {"dataset_views": dataset_views}
    except Exception as e:
        logger.error(f"Failed to fetch Explorer metrics: {e}")
        return {}


def get_superset_metrics(db: postgresql) -> dict:
    """Get metrics for Superset.

    Parameters
    ----------
    db : postgresql
        Database connection parameters for the Superset metastore database.

    Returns
    -------
    dict
        A dictionary containing metrics: dashboards, charts.
    """
    logger.info("Fetching Superset metrics...")

    try:
        conn_str = conninfo(db)
        with psycopg.connect(conn_str, autocommit=True) as conn:
            with conn.cursor() as cursor:
                # Count dashboards
                cursor.execute("""
                    SELECT COUNT(*)
                    FROM dashboards
                """)
                dashboards = cursor.fetchone()[0]

                # Count charts (slices table)
                cursor.execute("""
                    SELECT COUNT(*)
                    FROM slices
                """)
                charts = cursor.fetchone()[0]

        logger.info(f"Superset: {dashboards} dashboards, {charts} charts")

        return {
            "dashboards": dashboards,
            "charts": charts,
        }
    except Exception as e:
        logger.error(f"Failed to fetch Superset metrics: {e}")
        return {}


def get_files_metrics(datalake_path: str) -> dict:
    """Get metrics for files in the datalake.

    Parameters
    ----------
    datalake_path : str
        Path to the datalake directory.

    Returns
    -------
    dict
        A dictionary containing metrics: file_count, data_size_mb.
    """
    logger.info("Fetching files metrics...")

    try:
        path = Path(datalake_path)

        if not path.exists():
            logger.warning(f"Datalake path does not exist: {datalake_path}")
            return {}

        # Count all files recursively
        file_count = sum(1 for _ in path.rglob("*") if _.is_file())

        # Get total size
        data_size_bytes = get_directory_size(datalake_path)

        if data_size_bytes is None:
            logger.warning("Could not determine datalake size")
            return {"file_count": file_count}

        data_size_mb = round(data_size_bytes / (1024**2), 2)

        logger.info(f"Files: {file_count:,} files, {data_size_mb} MB total")

        return {
            "file_count": file_count,
            "data_size_mb": data_size_mb,
        }
    except Exception as e:
        logger.error(f"Failed to fetch files metrics: {e}")
        return {}


def get_auth0_metrics(auth0_resource: auth0, auth0_domain: str) -> dict:
    """Get metrics from Auth0 using the Management API.

    Parameters
    ----------
    auth0_resource : auth0
        An OAuth resource containing the tokenized access to Auth0 Management API.
    auth0_domain : str
        The Auth0 domain (e.g., "your-tenant.us.auth0.com").

    Returns
    -------
    dict
        A dictionary containing metrics: users (total number of users).
    """
    logger.info("Fetching Auth0 metrics...")

    try:
        # Auth0 Management API endpoint for users
        url = f"https://{auth0_domain}/api/v2/users"

        headers = {
            "Authorization": f"Bearer {auth0_resource['token']}",
        }

        # Get users with pagination support
        # The search_engine=v3 parameter is required for accurate totals
        params = {
            "search_engine": "v3",
            "per_page": 1,  # We only need the total count
            "include_totals": "true",
        }

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()

        data = response.json()
        total_users = data.get("total", 0)

        logger.info(f"Auth0: {total_users} users")

        return {"users": total_users}
    except Exception as e:
        logger.error(f"Failed to fetch Auth0 metrics: {e}")
        return {}


def get_windmill_metrics() -> dict:
    """Get metrics from Windmill using environment variables.

    When running inside a Windmill worker, the following environment variables
    are automatically available:
    - WM_TOKEN: Authentication token
    - WM_BASE_URL: Base URL of the Windmill instance
    - WM_WORKSPACE: Current workspace

    Returns
    -------
    dict
        A dictionary containing metrics: number_of_schedules.
        Returns empty dict if not running in Windmill or if there's an error.
    """
    logger.info("Fetching Windmill metrics using environment variables...")

    # Check if we're running in a Windmill worker
    base_url = os.environ.get("WM_BASE_URL")
    token = os.environ.get("WM_TOKEN")
    workspace = os.environ.get("WM_WORKSPACE")

    if not all([base_url, token, workspace]):
        logger.info(
            "Not running in Windmill worker (missing WM_* env vars), skipping Windmill metrics"
        )
        return {}

    try:
        # Get list of schedules from Windmill API
        url = f"{base_url}/api/w/{workspace}/schedules/list"

        headers = {
            "Authorization": f"Bearer {token}",
        }

        response = requests.get(url, headers=headers)
        response.raise_for_status()

        schedules = response.json()
        number_of_schedules = len(schedules) if isinstance(schedules, list) else 0

        logger.info(f"Windmill: {number_of_schedules} schedules")

        return {"number_of_schedules": number_of_schedules}
    except Exception as e:
        logger.error(f"Failed to fetch Windmill metrics: {e}")
        return {}


def main(
    comapeo: Optional[comapeo_server] = None,
    db: Optional[postgresql] = None,
    attachment_root: str = "/persistent-storage/datalake",
    guardianconnector_db: str = "guardianconnector",
    superset_db: str = "superset_metastore",
    auth0_resource: Optional[auth0] = None,
    auth0_domain: Optional[str] = None,
):
    """Generate Guardian Connector metrics based on provided parameters.

    All parameters are optional. The script will only collect metrics for services where the required parameters are provided.

    Windmill metrics are automatically collected when running inside a Windmill worker (using WM_* environment variables).

    Parameters
    ----------
    comapeo : comapeo_server, optional
        Dictionary containing 'server_url' and 'access_token' for the CoMapeo server.
        If not provided, CoMapeo metrics will be skipped.
    db : postgresql, optional
        Database connection parameters (will be used for all database connections with different database names).
        If not provided, all database metrics (warehouse, Explorer, Superset) will be skipped.
    attachment_root : str, optional
        Path to the datalake root directory where data is stored.
        Defaults to "/persistent-storage/datalake".
    guardianconnector_db : str, optional
        Database name for Explorer metrics. Uses 'db' connection parameters with this database name.
        Defaults to "guardianconnector".
    superset_db : str, optional
        Database name for Superset metrics. Uses 'db' connection parameters with this database name.
        Defaults to "superset_metastore".
    auth0_resource : auth0, optional
        OAuth resource with tokenized access to Auth0 Management API.
        If not provided, Auth0 metrics will be skipped.
    auth0_domain : str, optional
        The Auth0 domain (e.g., "your-tenant.us.auth0.com").
        Required if auth0_resource is provided.

    Returns
    -------
    dict
        A dictionary with service metrics nested under service names.
        Only includes metrics for services where required parameters were provided.
        Example: {
            "comapeo": {"project_count": 3, "data_size_mb": 100.5},
            "warehouse": {"tables": 50, "records": 1000000},
            "explorer": {"dataset_views": 11},
            "superset": {"dashboards": 5, "charts": 25},
            "files": {"file_count": 5000, "data_size_mb": 10000},
            "auth0": {"users": 150},
            "windmill": {"number_of_schedules": 15}
        }
    """
    metrics = {}

    # Get CoMapeo metrics (requires comapeo parameter)
    if comapeo:
        comapeo_metrics = get_comapeo_metrics(comapeo, attachment_root)
        if comapeo_metrics:
            metrics["comapeo"] = comapeo_metrics

    # Get warehouse metrics (requires db parameter)
    if db:
        warehouse_metrics = get_warehouse_metrics(db)
        if warehouse_metrics:
            metrics["warehouse"] = warehouse_metrics

        # Get explorer metrics (requires db parameter)
        guardianconnector_db_conn = {**db, "dbname": guardianconnector_db}
        explorer_metrics = get_explorer_metrics(guardianconnector_db_conn)
        if explorer_metrics:
            metrics["explorer"] = explorer_metrics

        # Get Superset metrics (requires db parameter)
        superset_db_conn = {**db, "dbname": superset_db}
        superset_metrics = get_superset_metrics(superset_db_conn)
        if superset_metrics:
            metrics["superset"] = superset_metrics

    # Get files metrics (only requires attachment_root which has a default)
    files_metrics = get_files_metrics(attachment_root)
    if files_metrics:
        metrics["files"] = files_metrics

    # Get Auth0 metrics (requires both auth0_resource and auth0_domain)
    if auth0_resource and auth0_domain:
        auth0_metrics = get_auth0_metrics(auth0_resource, auth0_domain)
        if auth0_metrics:
            metrics["auth0"] = auth0_metrics

    # Get Windmill metrics (automatically uses WM_* environment variables)
    windmill_metrics = get_windmill_metrics()
    if windmill_metrics:
        metrics["windmill"] = windmill_metrics

    return metrics
