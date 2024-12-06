import os

import pytest
from google.auth.credentials import AnonymousCredentials
from google.cloud import storage

pytest_plugins = ["f.frizzle.tests.conftest"]


@pytest.fixture
def gcs_emulator_client():
    """Return a google.cloud.storage.Client against an GCS emulator running in tox-docker"""
    # See https://github.com/tox-dev/tox-docker?tab=readme-ov-file#configuration, `expose` option
    TOX_DOCKER_GCS_PORT = os.environ["TOX_DOCKER_GCS_PORT"]
    emulator_endpoint = f"http://localhost:{TOX_DOCKER_GCS_PORT}"

    # Create a storage client that connects to the emulator
    storage_client = storage.Client(
        project="test-project",
        credentials=AnonymousCredentials(),
        client_options={"api_endpoint": emulator_endpoint},
    )

    yield storage_client

    # Clean up: Delete all buckets
    for bucket in storage_client.list_buckets():
        bucket.delete(force=True)
