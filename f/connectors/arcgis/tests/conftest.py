import json
import re
from dataclasses import dataclass

import pytest
import responses
import testing.postgresql

from f.connectors.arcgis.tests.assets import server_responses


@pytest.fixture
def mocked_responses():
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        yield rsps


@pytest.fixture
def arcgis_server(mocked_responses):
    """A mock ArcGIS Server that you can use to provide feature layer data"""

    @dataclass
    class ArcGISServer:
        account: dict
        feature_layer_url: str

    username = "my_username"
    password = "my_password"
    feature_layer_url = "https://services.arcgis.com/abc123/arcgis/rest/services/MyFeatureLayer/FeatureServer"

    mocked_responses.post(
        "https://www.arcgis.com/sharing/rest/generateToken",
        json=server_responses.arcgis_token(),
        status=200,
    )
    mocked_responses.get(
        f"{feature_layer_url}/0/query",
        json=server_responses.arcgis_features(),
        status=200,
    )
    mocked_responses.get(
        f"{feature_layer_url}/0/1/attachments",
        json=server_responses.arcgis_attachments(),
        status=200,
    )

    mocked_responses.get(
        re.compile(rf"{feature_layer_url}/0/1/attachments/1"),
        body=open(
            "f/connectors/arcgis/tests/assets/springfield_photo.png", "rb"
        ).read(),
        content_type="image/png",
    )

    mocked_responses.get(
        re.compile(rf"{feature_layer_url}/0/1/attachments/2"),
        body=open(
            "f/connectors/arcgis/tests/assets/springfield_audio.mp4", "rb"
        ).read(),
        content_type="video/mp4",
    )

    account = dict(username=username, password=password)

    return ArcGISServer(
        account,
        feature_layer_url,
    )


@pytest.fixture
def arcgis_anonymous_server(mocked_responses):
    """A mock ArcGIS Server for anonymous access (no authentication required)"""

    @dataclass
    class ArcGISAnonymousServer:
        subdomain: str
        service_id: str
        feature_id: str
        base_url: str

    subdomain = "services"
    service_id = "abc123"
    feature_id = "MyAnonymousLayer"
    base_url = f"https://{subdomain}.arcgis.com/{service_id}/arcgis/rest/services/{feature_id}/FeatureServer"

    # Metadata endpoint
    mocked_responses.get(
        f"{base_url}?f=pjson",
        json=server_responses.arcgis_metadata_anonymous(),
        status=200,
    )

    # Query endpoint for layer 0 - use callback to handle pagination
    call_count = {"query": 0}

    def query_callback(request):
        call_count["query"] += 1
        if call_count["query"] == 1:
            # First call returns features
            return (200, {}, json.dumps(server_responses.arcgis_features_anonymous()))
        else:
            # Subsequent calls return empty to stop pagination
            return (
                200,
                {},
                json.dumps(
                    {
                        "features": [],
                        "objectIdFieldName": "OBJECTID",
                        "geometryType": "esriGeometryPoint",
                    }
                ),
            )

    mocked_responses.add_callback(
        responses.GET,
        re.compile(rf"{re.escape(base_url)}/0/query(\?.*)?"),
        callback=query_callback,
    )

    # Attachments list endpoint
    mocked_responses.get(
        re.compile(rf"{re.escape(base_url)}/0/1/attachments(\?.*)?"),
        json=server_responses.arcgis_attachments(),
        status=200,
    )

    # Attachment downloads
    mocked_responses.get(
        re.compile(rf"{re.escape(base_url)}/0/1/attachments/1(\?.*)?"),
        body=open(
            "f/connectors/arcgis/tests/assets/springfield_photo.png", "rb"
        ).read(),
        content_type="image/png",
    )

    mocked_responses.get(
        re.compile(rf"{re.escape(base_url)}/0/1/attachments/2(\?.*)?"),
        body=open(
            "f/connectors/arcgis/tests/assets/springfield_audio.mp4", "rb"
        ).read(),
        content_type="video/mp4",
    )

    return ArcGISAnonymousServer(
        subdomain=subdomain,
        service_id=service_id,
        feature_id=feature_id,
        base_url=base_url,
    )


@pytest.fixture
def pg_database():
    """A dsn that may be used to connect to a live (local for test) postgresql server"""
    db = testing.postgresql.Postgresql(port=7654)
    dsn = db.dsn()
    dsn["dbname"] = dsn.pop("database")
    yield dsn
    db.stop
