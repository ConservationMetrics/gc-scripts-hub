from pathlib import Path

import pytest
import responses

from f.connectors.gbif.tests.assets import server_responses


@pytest.fixture
def mocked_responses():
    with responses.RequestsMock() as mocked:
        yield mocked


@pytest.fixture
def archive_bytes():
    return (
        Path(__file__).parent / "assets" / "gbif-download-20260917.zip"
    ).read_bytes()


@pytest.fixture
def statistics_snapshot():
    return server_responses.statistics_snapshot()


@pytest.fixture
def pg_database(postgresql_factory, monkeypatch, tmp_path):
    from psycopg import connect

    db = postgresql_factory()
    dsn = db.dsn()
    dsn["dbname"] = dsn.pop("database")
    monkeypatch.setenv("DATASET_IMPORTER_DATALAKE_ROOT", str(tmp_path))
    with connect(**dsn) as conn:
        conn.execute("""
            CREATE SCHEMA registry;
            CREATE TABLE registry.datasets (
                id uuid PRIMARY KEY DEFAULT gen_random_uuid(), schema_name text NOT NULL,
                table_name text NOT NULL, created_at timestamptz, last_imported_at timestamptz,
                last_updated_at timestamptz, UNIQUE (schema_name, table_name)
            );
            CREATE TABLE registry.files (
                id uuid PRIMARY KEY DEFAULT gen_random_uuid(), relative_path text NOT NULL,
                size_bytes bigint NOT NULL, media_type text, content_sha256 text,
                filesystem_modified_at timestamptz, last_seen_at timestamptz DEFAULT now(),
                missing_since timestamptz, deleted_at timestamptz, display_name text,
                source_metadata jsonb NOT NULL DEFAULT '{}'::jsonb
            );
            CREATE UNIQUE INDEX ON registry.files (relative_path) WHERE deleted_at IS NULL;
            CREATE TABLE registry.ingest_runs (
                id uuid PRIMARY KEY DEFAULT gen_random_uuid(), connector_id text NOT NULL,
                started_at timestamptz DEFAULT now(), completed_at timestamptz,
                outcome text NOT NULL DEFAULT 'running'
            );
            CREATE TABLE registry.ingest_runs_datasets (
                run_id uuid REFERENCES registry.ingest_runs, dataset_id uuid REFERENCES registry.datasets,
                operation text NOT NULL, recorded_at timestamptz DEFAULT now(), PRIMARY KEY (run_id, dataset_id)
            );
            CREATE TABLE registry.ingest_runs_files (
                run_id uuid REFERENCES registry.ingest_runs, file_id uuid REFERENCES registry.files,
                operation text NOT NULL, recorded_at timestamptz DEFAULT now(), PRIMARY KEY (run_id, file_id)
            );
            CREATE TABLE registry.datasets_files (
                dataset_id uuid REFERENCES registry.datasets, file_id uuid REFERENCES registry.files,
                linked_at timestamptz DEFAULT now(), PRIMARY KEY (dataset_id, file_id)
            )
        """)
    yield dsn
    db.stop()
