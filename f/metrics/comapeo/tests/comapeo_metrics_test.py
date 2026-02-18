from f.metrics.comapeo.comapeo_metrics import main


def test_project_count(comapeo_server_fixture):
    """Test that the script correctly counts projects on a CoMapeo server."""
    
    result = main(comapeo_server_fixture)
    
    assert "project_count" in result
    assert result["project_count"] == 3


def test_project_count_empty(mocked_responses):
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
    
    result = main(comapeo)
    
    assert result["project_count"] == 0

