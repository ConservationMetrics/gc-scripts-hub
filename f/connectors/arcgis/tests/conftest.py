import re
from dataclasses import dataclass

import pytest
import responses
import testing.postgresql

from f.connectors.arcgis.tests.assets import server_responses


@pytest.fixture
def mocked_responses():
    with responses.RequestsMock() as rsps:
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
def pg_database():
    """A dsn that may be used to connect to a live (local for test) postgresql server"""
    db = testing.postgresql.Postgresql(port=7654)
    dsn = db.dsn()
    dsn["dbname"] = dsn.pop("database")
    yield dsn
    db.stop
