from unittest.mock import patch

import psycopg
import responses

from f.common_logic.db_operations import conninfo
from f.metrics.guardianconnector.guardianconnector_metrics import (
    _flatten_metrics,
    get_auth0_metrics,
    get_comapeo_metrics,
    get_datalake_metrics,
    get_directory_size,
    get_explorer_metrics,
    get_superset_metrics,
    get_warehouse_metrics,
    get_windmill_metrics,
    main,
)


def test_get_directory_size(tmp_path):
    """Test the directory size calculation function."""

    # Create a directory with known content
    test_dir = tmp_path / "test_dir"
    test_dir.mkdir()
    (test_dir / "file1.txt").write_bytes(b"x" * 1000)
    (test_dir / "file2.txt").write_bytes(b"y" * 2000)

    size = get_directory_size(str(test_dir))

    assert size is not None
    assert size > 3000  # At least the size of our files


def test_get_directory_size_nonexistent():
    """Test directory size function with nonexistent path."""

    size = get_directory_size("/nonexistent/path")

    assert size is None


@patch("f.metrics.guardianconnector.guardianconnector_metrics.subprocess.run")
def test_get_directory_size_subprocess_error(mock_run, tmp_path):
    """Test directory size function handles subprocess errors."""

    test_dir = tmp_path / "test_dir"
    test_dir.mkdir()

    # Simulate subprocess error
    mock_run.side_effect = Exception("Command failed")

    size = get_directory_size(str(test_dir))

    assert size is None


def test_get_comapeo_metrics(comapeo_server_fixture, tmp_path):
    """Test the CoMapeo metrics function directly."""

    # Create a temporary directory to simulate the datalake
    datalake_root = tmp_path / "datalake"
    comapeo_dir = datalake_root / "comapeo"
    comapeo_dir.mkdir(parents=True)
    (comapeo_dir / "test_file.txt").write_bytes(b"x" * (1024 * 1024))

    result = get_comapeo_metrics(comapeo_server_fixture, str(datalake_root))

    assert "project_count" in result
    assert result["project_count"] == 3
    assert "data_size_mb" in result
    assert result["data_size_mb"] >= 1.0


def test_comapeo_data_size_nonexistent_path(comapeo_server_fixture, pg_database):
    """Test that the script handles nonexistent data paths gracefully."""

    result = main(
        comapeo_server_fixture,
        pg_database,
        "/nonexistent/path",
        guardianconnector_db="test",
        superset_db="test",
    )

    assert "comapeo" in result
    assert result["comapeo"]["project_count"] == 3
    # Data size metric should not be present
    assert "data_size_mb" not in result["comapeo"]


def test_get_warehouse_metrics(pg_database):
    """Test warehouse metrics collection."""

    metrics = get_warehouse_metrics(pg_database)

    assert "tables" in metrics
    assert "records" in metrics
    # Should have 5 tables (test_table_1, test_table_2, view_config, dashboards, slices)
    assert metrics["tables"] == 5
    # Should have 17 total records (2 + 3 + 3 + 2 + 4 + 3)
    assert metrics["records"] >= 14


def test_get_explorer_metrics(pg_database):
    """Test explorer metrics collection."""

    metrics = get_explorer_metrics(pg_database)

    assert "dataset_views" in metrics
    # Should have 3 records in view_config
    assert metrics["dataset_views"] == 3


def test_get_superset_metrics(pg_database):
    """Test Superset metrics collection."""

    metrics = get_superset_metrics(pg_database)

    assert "dashboards" in metrics
    assert "charts" in metrics
    # Should have 2 dashboards
    assert metrics["dashboards"] == 2
    # Should have 4 charts (slices)
    assert metrics["charts"] == 4


def test_get_datalake_metrics(tmp_path):
    """Test datalake metrics collection."""
    # Create a test datalake structure
    datalake = tmp_path / "datalake"
    datalake.mkdir()

    # Create some test files
    (datalake / "file1.txt").write_bytes(b"x" * (1024 * 1024))  # 1MB
    (datalake / "file2.txt").write_bytes(b"y" * (512 * 1024))  # 512KB
    subdir = datalake / "subdir"
    subdir.mkdir()
    (subdir / "file3.txt").write_bytes(b"z" * (1024 * 1024))  # 1MB

    metrics = get_datalake_metrics(str(datalake))

    assert "file_count" in metrics
    assert "data_size_mb" in metrics
    assert metrics["file_count"] == 3
    assert metrics["data_size_mb"] >= 2.5  # At least 2.5MB


def test_get_windmill_metrics():
    """Test Windmill metrics collection using environment variables."""
    # Mock environment variables
    with patch.dict(
        "os.environ",
        {
            "WM_BASE_URL": "http://windmill.example.org",
            "WM_TOKEN": "test_token",
            "WM_WORKSPACE": "test_workspace",
        },
    ):
        with responses.RequestsMock() as rsps:
            rsps.add(
                responses.GET,
                "http://windmill.example.org/api/w/test_workspace/schedules/list",
                json=[
                    {"path": "f/example/script1", "schedule": "0 0 * * *"},
                    {"path": "f/example/script2", "schedule": "0 12 * * *"},
                    {"path": "f/example/script3", "schedule": "*/15 * * * *"},
                ],
                status=200,
            )

            metrics = get_windmill_metrics()

            assert "number_of_schedules" in metrics
            assert metrics["number_of_schedules"] == 3


def test_get_auth0_metrics():
    """Test Auth0 metrics collection."""
    auth0_resource = {"token": "test_auth0_token"}
    auth0_domain = "your-tenant.us.auth0.com"

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"https://{auth0_domain}/api/v2/users",
            json={
                "users": [{"user_id": "auth0|123"}],  # Actual users list
                "total": 150,  # Total count
            },
            status=200,
        )

        metrics = get_auth0_metrics(auth0_resource, auth0_domain)

        assert "users" in metrics
        assert metrics["users"] == 150


def test_flatten_metrics():
    """Test that metrics are correctly flattened with double underscore separator."""
    metrics = {
        "comapeo": {"project_count": 3, "data_size_mb": 100.5},
        "warehouse": {"tables": 50, "records": 1000000},
        "datalake": {"file_count": 5000, "data_size_mb": 10000.0},
    }
    date_str = "2026-02-18"

    flattened = _flatten_metrics(metrics, date_str)

    # Check _id and date
    assert flattened["_id"] == "20260218"
    assert flattened["date"] == "2026-02-18"

    # Check flattened metrics
    assert flattened["comapeo__project_count"] == 3
    assert flattened["comapeo__data_size_mb"] == 100.5
    assert flattened["warehouse__tables"] == 50
    assert flattened["warehouse__records"] == 1000000
    assert flattened["datalake__file_count"] == 5000
    assert flattened["datalake__data_size_mb"] == 10000.0


def test_guardianconnector_full_metrics_and_db_write(
    comapeo_server_fixture, pg_database, tmp_path
):
    """Test full metrics collection, structure, and database persistence."""
    # Create a temporary directory to simulate the datalake
    datalake_root = tmp_path / "datalake"
    comapeo_dir = datalake_root / "comapeo"
    comapeo_dir.mkdir(parents=True)
    # Create a file large enough to show up in MB (1 MB)
    (comapeo_dir / "test_file.txt").write_bytes(b"x" * (1024 * 1024))

    # Pass the test database name for both guardianconnector_db and superset_db
    result = main(
        comapeo_server_fixture,
        pg_database,
        str(datalake_root),
        guardianconnector_db="test",
        superset_db="test",
    )

    # Check top-level structure
    assert isinstance(result, dict)
    assert "comapeo" in result
    assert isinstance(result["comapeo"], dict)

    # Check all services are present
    assert "warehouse" in result
    assert "explorer" in result
    assert "superset" in result
    assert "datalake" in result

    # Check CoMapeo metrics
    assert "project_count" in result["comapeo"]
    assert result["comapeo"]["project_count"] == 3
    assert "data_size_mb" in result["comapeo"]
    assert result["comapeo"]["data_size_mb"] >= 1.0

    # Check warehouse metrics
    assert result["warehouse"]["tables"] == 5
    assert result["warehouse"]["records"] >= 14

    # Check explorer metrics
    assert result["explorer"]["dataset_views"] == 3

    # Check Superset metrics
    assert result["superset"]["dashboards"] == 2
    assert result["superset"]["charts"] == 4

    # Check datalake metrics
    assert "file_count" in result["datalake"]
    assert "data_size_mb" in result["datalake"]
    assert result["datalake"]["file_count"] >= 1  # At least the test file
    assert result["datalake"]["data_size_mb"] >= 1.0  # At least 1MB

    # Verify metrics were written to database
    conn_str = conninfo({**pg_database, "dbname": "test"})
    with psycopg.connect(conn_str, autocommit=True) as conn:
        with conn.cursor() as cursor:
            # Check that metrics table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = 'metrics'
                )
            """)
            assert cursor.fetchone()[0] is True

            # Check that a row was inserted
            cursor.execute("SELECT COUNT(*) FROM metrics")
            assert cursor.fetchone()[0] == 1

            # Check the row content
            cursor.execute(
                "SELECT _id, date, comapeo__project_count, warehouse__tables, datalake__file_count FROM metrics"
            )
            row = cursor.fetchone()
            assert row is not None
            _id, date, comapeo_projects, warehouse_tables, files_count = row

            # Verify _id format (YYYYMMDD)
            assert len(_id) == 8
            assert _id.isdigit()

            # Verify date
            assert date is not None

            # Verify metrics values match what was returned
            assert comapeo_projects == 3
            assert warehouse_tables == 5
            assert files_count >= 1


def test_metrics_update_on_same_day(comapeo_server_fixture, pg_database, tmp_path):
    """Test that running the script twice on the same day updates the existing row."""
    # Create test datalake
    datalake_root = tmp_path / "datalake"
    comapeo_dir = datalake_root / "comapeo"
    comapeo_dir.mkdir(parents=True)
    (comapeo_dir / "test_file.txt").write_bytes(b"x" * (1024 * 1024))

    # Run main twice
    main(
        comapeo_server_fixture,
        pg_database,
        str(datalake_root),
        guardianconnector_db="test",
        superset_db="test",
    )
    main(
        comapeo_server_fixture,
        pg_database,
        str(datalake_root),
        guardianconnector_db="test",
        superset_db="test",
    )

    # Verify only one row exists
    conn_str = conninfo({**pg_database, "dbname": "test"})
    with psycopg.connect(conn_str, autocommit=True) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM metrics")
            assert cursor.fetchone()[0] == 1


def test_no_parameters_provided(tmp_path):
    """Test that the script works when no parameters are provided."""
    # Create a datalake directory for datalake metrics
    datalake_root = tmp_path / "datalake"
    datalake_root.mkdir()
    (datalake_root / "test_file.txt").write_bytes(b"x" * (1024 * 1024))

    # Run with no CoMapeo, no db, no Windmill - only datalake metrics
    result = main(attachment_root=str(datalake_root))

    # Only datalake metrics should be present
    assert "datalake" in result
    assert result["datalake"]["file_count"] >= 1
    assert result["datalake"]["data_size_mb"] >= 1.0

    # All other metrics should not be present
    assert "comapeo" not in result
    assert "warehouse" not in result
    assert "explorer" not in result
    assert "superset" not in result
    assert "windmill" not in result

    # Result should not be empty
    assert len(result) == 1
