from unittest.mock import patch

from f.metrics.comapeo.comapeo_metrics import get_directory_size, main


def test_project_count(comapeo_server_fixture, tmp_path):
    """Test that the script correctly counts projects on a CoMapeo server."""
    
    # Create a temporary directory to simulate the datalake
    datalake_root = tmp_path / "datalake"
    comapeo_dir = datalake_root / "comapeo"
    comapeo_dir.mkdir(parents=True)
    # Create a file large enough to show up in MB (1 MB)
    (comapeo_dir / "test_file.txt").write_bytes(b"x" * (1024 * 1024))
    
    result = main(comapeo_server_fixture, str(datalake_root))
    
    assert "project_count" in result
    assert result["project_count"] == 3
    assert "data_size_mb" in result
    assert result["data_size_mb"] >= 1.0


def test_project_count_empty(mocked_responses, tmp_path):
    """Test that the script handles an empty project list."""
    
    server_url = "http://comapeo.example.org"
    access_token = "test_token"
    
    # Mock empty projects response
    mocked_responses.get(
        f"{server_url}/projects",
        json={"data": []},
        status=200,
    )
    
    comapeo = {
        "server_url": server_url,
        "access_token": access_token,
    }
    
    # Create a temporary directory to simulate the datalake
    datalake_root = tmp_path / "datalake"
    comapeo_dir = datalake_root / "comapeo"
    comapeo_dir.mkdir(parents=True)
    
    result = main(comapeo, str(datalake_root))
    
    assert result["project_count"] == 0
    assert "data_size_mb" in result


def test_data_size_nonexistent_path(comapeo_server_fixture):
    """Test that the script handles nonexistent data paths gracefully."""
    
    result = main(comapeo_server_fixture, "/nonexistent/path")
    
    assert "project_count" in result
    assert result["project_count"] == 3
    # Data size metric should not be present
    assert "data_size_mb" not in result


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


@patch("f.metrics.comapeo.comapeo_metrics.subprocess.run")
def test_get_directory_size_subprocess_error(mock_run, tmp_path):
    """Test directory size function handles subprocess errors."""
    
    test_dir = tmp_path / "test_dir"
    test_dir.mkdir()
    
    # Simulate subprocess error
    mock_run.side_effect = Exception("Command failed")
    
    size = get_directory_size(str(test_dir))
    
    assert size is None

