import pytest
from gcp_storage_emulator.server import create_server
from google.auth.credentials import AnonymousCredentials
from google.cloud import storage


@pytest.fixture
def pg_database(postgresql_factory):
    """A dsn that may be used to connect to a live (local for test) postgresql server"""
    db = postgresql_factory()
    dsn = db.dsn()
    dsn["dbname"] = dsn.pop("database")
    yield dsn
    db.stop()


@pytest.fixture
def gcs_emulator_client():
    """Return a google.cloud.storage.Client connected to a local GCS emulator."""
    host = "127.0.0.1"
    # Port 0 lets the OS select and bind a free port atomically, avoiding an allocation race.
    emulator_server = create_server(host, 0, in_memory=True)
    emulator_server.start()
    try:
        port = emulator_server._api._httpd.server_address[1]
        storage_client = storage.Client(
            project="test-project",
            credentials=AnonymousCredentials(),
            client_options={"api_endpoint": f"http://{host}:{port}"},
        )
        try:
            yield storage_client
        finally:
            storage_client.close()
    finally:
        emulator_server.stop()
