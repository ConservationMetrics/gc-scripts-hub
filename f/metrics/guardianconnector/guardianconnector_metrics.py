# requirements:
# requests~=2.32
# psycopg[binary]

import logging
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Optional, TypedDict

import psycopg
import requests
from psycopg import sql

from f.common_logic.db_operations import (
    StructuredDBWriter,
    conninfo,
    create_database_if_not_exists,
    postgresql,
)


class comapeo_server(TypedDict):
    server_url: str
    access_token: str


# https://hub.windmill.dev/resource_types/337/oauth_application
class oauth_application(TypedDict):
    client_id: str
    client_secret: str
    domain: str


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(
    comapeo: Optional[comapeo_server] = None,
    db: Optional[postgresql] = None,
    attachment_root: Optional[str] = None,
    superset_db: str = "superset_metastore",
    oauth_application: Optional[oauth_application] = None,
):
    metrics = {}
    guardianconnector_db = (
        "guardianconnector"  # This is always the database name for Explorer and metrics
    )

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

    # Get datalake metrics (only if attachment_root is provided and not blank)
    if attachment_root:
        datalake_metrics = get_datalake_metrics(attachment_root)
        if datalake_metrics:
            metrics["datalake"] = datalake_metrics

    # Get Auth0 metrics (requires oauth_application parameter)
    if oauth_application:
        auth0_metrics = get_auth0_metrics(oauth_application)
        if auth0_metrics:
            metrics["auth0"] = auth0_metrics

    # Get Windmill metrics (automatically uses WM_* environment variables)
    windmill_metrics = get_windmill_metrics()
    if windmill_metrics:
        metrics["windmill"] = windmill_metrics

    # Write metrics to database if db parameter provided
    if db and metrics:
        try:
            # Ensure guardianconnector database exists
            create_database_if_not_exists(db, guardianconnector_db)

            date_str = datetime.now().strftime("%Y-%m-%d")
            flattened_metrics = _flatten_metrics(metrics, date_str)

            guardianconnector_db_conn = {**db, "dbname": guardianconnector_db}
            metrics_writer = StructuredDBWriter(
                conninfo(guardianconnector_db_conn),
                "metrics",
                predefined_schema=_create_metrics_table,
            )
            metrics_writer.handle_output([flattened_metrics])
            logger.info(f"Successfully wrote metrics to database for {date_str}")
        except Exception as e:
            logger.error(f"Failed to write metrics to database: {e}")

    return metrics


def get_comapeo_metrics(
    comapeo: comapeo_server,
    attachment_root: Optional[str] = None,
) -> dict:
    """Get metrics for CoMapeo server.

    Parameters
    ----------
    comapeo : comapeo_server
        Dictionary containing 'server_url' and 'access_token' for the CoMapeo server.
    attachment_root : str, optional
        Path to the datalake root directory where CoMapeo data is stored.
        If not provided, data size calculation will be skipped.

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

    metrics = {"project_count": project_count}

    # Get size of comapeo data directory if attachment_root is provided
    if attachment_root:
        comapeo_data_path = Path(attachment_root) / "comapeo"
        data_size_bytes = get_directory_size(str(comapeo_data_path))

        if data_size_bytes is not None:
            data_size_mb = round(data_size_bytes / (1024**2), 2)
            metrics["data_size_mb"] = data_size_mb
            logger.info(
                f"CoMapeo data directory size (on datalake/comapeo dir, not the CoMapeo volume): {data_size_mb} MB"
            )
        else:
            logger.warning("Could not determine data directory size")
    else:
        logger.info(
            "Skipping CoMapeo data size calculation (no attachment_root provided)"
        )

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


def get_datalake_metrics(datalake_path: str) -> dict:
    """Get metrics for the datalake.

    Parameters
    ----------
    datalake_path : str
        Path to the datalake directory.

    Returns
    -------
    dict
        A dictionary containing metrics: file_count, data_size_mb.
    """
    logger.info("Fetching datalake metrics...")

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
        logger.error(f"Failed to fetch datalake metrics: {e}")
        return {}


def get_auth0_metrics(oauth_application: oauth_application) -> dict:
    """Get metrics from Auth0 using the Management API.

    Parameters
    ----------
    oauth_application : oauth_application
        Dictionary containing 'client_id', 'client_secret', and 'domain'
        for Auth0 M2M application.

    Returns
    -------
    dict
        A dictionary containing metrics: users, users_signed_in_past_30_days, logins.
    """
    logger.info("Fetching Auth0 metrics...")

    auth0_domain = oauth_application["domain"]

    try:
        # Step 1: Get Management API access token using client credentials flow
        token_url = f"https://{auth0_domain}/oauth/token"
        token_payload = {
            "client_id": oauth_application["client_id"],
            "client_secret": oauth_application["client_secret"],
            "audience": f"https://{auth0_domain}/api/v2/",
            "grant_type": "client_credentials",
        }

        token_response = requests.post(token_url, json=token_payload)
        token_response.raise_for_status()
        token_data = token_response.json()
        access_token = token_data.get("access_token")

        if not access_token:
            logger.error("Failed to obtain access token from Auth0")
            return {}

        # Step 2: Query Management API for metrics
        headers = {
            "Authorization": f"Bearer {access_token}",
        }

        metrics = {}

        # Get total users
        users_url = f"https://{auth0_domain}/api/v2/users"
        users_params = {
            "per_page": 1,
            "include_totals": "true",
        }
        users_response = requests.get(users_url, headers=headers, params=users_params)
        users_response.raise_for_status()
        total_users = users_response.json().get("total", 0)
        metrics["users"] = total_users

        # Get users signed in past 30 days
        from datetime import datetime, timedelta, timezone

        thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=30)
        last_login_query = (
            f"last_login:[{thirty_days_ago.strftime('%Y-%m-%dT%H:%M:%S.000Z')} TO *]"
        )

        active_users_params = {
            "q": last_login_query,
            "search_engine": "v3",
            "per_page": 1,
            "include_totals": "true",
        }
        active_users_response = requests.get(
            users_url, headers=headers, params=active_users_params
        )
        active_users_response.raise_for_status()
        users_signed_in_past_30_days = active_users_response.json().get("total", 0)
        metrics["users_signed_in_past_30_days"] = users_signed_in_past_30_days

        # Get total logins across all users by paginating through users
        total_logins = 0
        page = 0
        per_page = 100  # Max allowed by Auth0

        while True:
            logins_params = {
                "per_page": per_page,
                "page": page,
                "fields": "logins_count",
                "include_fields": "true",
            }
            logins_response = requests.get(
                users_url, headers=headers, params=logins_params
            )
            logins_response.raise_for_status()
            users_data = logins_response.json()

            if not users_data:
                break

            # Sum up logins_count for this page
            page_logins = sum(user.get("logins_count", 0) for user in users_data)
            total_logins += page_logins

            # If we got fewer users than requested, we've reached the end
            if len(users_data) < per_page:
                break

            page += 1

        metrics["logins"] = total_logins

        logger.info(
            f"Auth0: {total_users} users, {users_signed_in_past_30_days} active in past 30 days, {total_logins} total logins"
        )

        return metrics
    except requests.exceptions.HTTPError as e:
        logger.error(f"Failed to fetch Auth0 metrics: {e}")
        if hasattr(e.response, "text"):
            logger.error(f"Auth0 API response: {e.response.text}")
        return {}
    except Exception as e:
        logger.error(f"Failed to fetch Auth0 metrics: {e}")
        return {}


def get_windmill_metrics() -> dict:
    """Get metrics from Windmill using environment variables.

    Inside a Windmill worker, the following environment variables
    are automatically available:
    - WM_TOKEN: Authentication token
    - WM_BASE_URL: Base URL of the Windmill instance
    - WM_WORKSPACE: Current workspace

    Returns
    -------
    dict
        A dictionary containing metrics: schedules.
        Returns empty dict if not running in Windmill or if there's an error.
    """
    logger.info("Fetching Windmill metrics using environment variables...")

    base_url = os.environ.get("WM_BASE_URL")
    token = os.environ.get("WM_TOKEN")
    workspace = os.environ.get("WM_WORKSPACE")

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

        return {"schedules": number_of_schedules}
    except Exception as e:
        logger.error(f"Failed to fetch Windmill metrics: {e}")
        return {}


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


def _flatten_metrics(metrics: dict, date_str: str) -> dict:
    """Flatten nested metrics dictionary with double underscore separator.

    Parameters
    ----------
    metrics : dict
        Nested dictionary of metrics by service.
    date_str : str
        Date string in YYYY-MM-DD format.

    Returns
    -------
    dict
        Flattened dictionary with keys like 'files__file_count', '_id', and 'date'.
    """
    flattened = {
        "_id": date_str.replace("-", ""),  # YYYYMMDD format
        "date": date_str,
    }

    for service, service_metrics in metrics.items():
        for metric_name, metric_value in service_metrics.items():
            flattened[f"{service}__{metric_name}"] = metric_value

    return flattened


def _create_metrics_table(cursor, table_name: str):
    """Create the metrics table if it doesn't exist.

    Parameters
    ----------
    cursor : psycopg.Cursor
        Database cursor.
    table_name : str
        Name of the table to create.
    """
    cursor.execute(
        sql.SQL("""
        CREATE TABLE IF NOT EXISTS {table} (
            _id text PRIMARY KEY,
            date date NOT NULL,
            -- CoMapeo metrics
            comapeo__project_count integer,
            comapeo__data_size_mb numeric,
            -- Warehouse metrics
            warehouse__tables integer,
            warehouse__records integer,
            -- Explorer metrics
            explorer__dataset_views integer,
            -- Superset metrics
            superset__dashboards integer,
            superset__charts integer,
            -- Datalake metrics
            datalake__file_count integer,
            datalake__data_size_mb numeric,
            -- Auth0 metrics
            auth0__users integer,
            auth0__users_signed_in_past_30_days integer,
            auth0__logins integer,
            -- Windmill metrics
            windmill__schedules integer
        );
    """).format(table=sql.Identifier(table_name))
    )
