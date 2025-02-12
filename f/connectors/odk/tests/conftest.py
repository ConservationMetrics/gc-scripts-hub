import re
from dataclasses import dataclass

import pytest
import responses
import testing.postgresql

from f.connectors.odk.tests.assets import server_responses


@pytest.fixture
def mocked_responses():
    """responses.RequestsMock context, for testing code that makes HTTP requests."""
    with responses.RequestsMock() as rsps:
        yield rsps


@pytest.fixture
def odkserver(mocked_responses):
    """A mock ODK Server that you can use to provide survey responses.

    Note: While odk_responses utilizes pyODK as a client to interact with ODK Central,
    it ultimately makes HTTP requests to the ODK Central API. This fixture mocks the
    ODK Central server responses.
    """

    @dataclass
    class OdkServer:
        config: dict
        form_id: str

    base_url = "http://odk.example.org"
    default_project_id = "1"

    form_id = "My_monitoring_form"

    mocked_responses.post(
        f"{base_url}/v1/sessions",
        json={"token": "mocked_token"},
        status=200,
    )

    mocked_responses.get(
        f"{base_url}/v1/projects/{default_project_id}/forms/{form_id}.svc/Submissions",
        json=server_responses.odk_form_submissions(),
        status=200,
    )

    mocked_responses.get(
        f"{base_url}/v1/projects/{default_project_id}/forms/{form_id}/submissions/uuid:24951a9e-db46-4e22-9bce-910377c9dd22/attachments",
        json=server_responses.odk_form_submission_attachments(),
        status=200,
    )

    mocked_responses.get(
        re.compile(
            rf"{base_url}/v1/projects/{default_project_id}/forms/{form_id}/submissions/uuid:24951a9e-db46-4e22-9bce-910377c9dd22/attachments/1739327186781.m4a"
        ),
        body=open("f/connectors/odk/tests/assets/1739327186781.m4a", "rb").read(),
        content_type="audio/m4a",
        headers={"Content-Length": "3632"},
    )

    return OdkServer(
        dict(
            base_url=base_url,
            username="collector",
            password="GathererOfData",
            default_project_id=default_project_id,
        ),
        form_id,
    )


@pytest.fixture
def pg_database():
    """A dsn that may be used to connect to a live (local for test) postgresql server"""
    db = testing.postgresql.Postgresql(port=7654)
    dsn = db.dsn()
    dsn["dbname"] = dsn.pop("database")
    yield dsn
    db.stop
