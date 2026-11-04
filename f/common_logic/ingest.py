# requirements:
# psycopg[binary]

"""Registry operations shared by connector ingest implementations.

The registry schema is provisioned separately. Dataset changes and their
registry records use the caller's transaction; run status uses a separate one
so a failed transaction can still be reported.
"""

import hashlib
import logging
import mimetypes
import os
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from psycopg import connect
from psycopg.types.json import Jsonb

from f.common_logic.db_operations import (
    StructuredDBWriter,
    conninfo,
    resolve_db_table_name,
)

logger = logging.getLogger(__name__)


def datalake_root():
    """Return the local mount of the deployment's logical datalake."""
    return Path(
        os.environ.get("DATASET_IMPORTER_DATALAKE_ROOT", "/persistent-storage/datalake")
    )


def datalake_relative_path(path):
    """Resolve a path within the configured datalake and return its relative path."""
    path = Path(path).resolve()
    try:
        return path.relative_to(datalake_root().resolve()).as_posix()
    except ValueError as exc:
        raise ValueError(f"Path is outside the configured datalake: {path}") from exc


@contextmanager
def ingest_run(db, connector_id):
    """Record one execution; propagate errors after recording failure."""
    dsn = conninfo(db)
    with connect(dsn) as conn:
        run_id = conn.execute(
            "INSERT INTO registry.ingest_runs (connector_id) VALUES (%s) RETURNING id",
            (connector_id,),
        ).fetchone()[0]
    try:
        yield run_id
    except BaseException:
        try:
            with connect(dsn) as conn:
                conn.execute(
                    "UPDATE registry.ingest_runs SET outcome = 'failed', completed_at = now() WHERE id = %s",
                    (run_id,),
                )
        except Exception:
            logger.exception("Could not record failure for ingest run %s", run_id)
        raise


def complete_run(cursor, run_id):
    """Commit successful status together with the registered resources."""
    cursor.execute(
        "UPDATE registry.ingest_runs SET outcome = 'succeeded', completed_at = now() WHERE id = %s",
        (run_id,),
    )


def require_known_file_hashes(db, paths):
    """Reject overwrites whose previously recorded content cannot be compared."""
    relative_paths = [datalake_relative_path(path) for path in paths]
    with connect(conninfo(db)) as conn:
        row = conn.execute(
            "SELECT relative_path FROM registry.files WHERE relative_path = ANY(%s) AND deleted_at IS NULL AND content_sha256 IS NULL LIMIT 1",
            (relative_paths,),
        ).fetchone()
    if row:
        raise ValueError(f"Previously registered file lacks a checksum: {row[0]}")


def register_dataset(cursor, run_id, table_name, operation):
    """Record a successfully written dataset inside the caller's transaction."""
    cursor.execute(
        """INSERT INTO registry.datasets (schema_name, table_name, created_at, last_imported_at, last_updated_at)
        VALUES ('public', %s, CASE WHEN %s = 'created' THEN now() END, now(),
                CASE WHEN %s IN ('created', 'updated') THEN now() END)
        ON CONFLICT (schema_name, table_name) DO UPDATE SET
            last_imported_at = now(),
            last_updated_at = CASE WHEN %s IN ('created', 'updated') THEN now()
                                   ELSE registry.datasets.last_updated_at END
        RETURNING id""",
        (table_name, operation, operation, operation),
    )
    dataset_id = cursor.fetchone()[0]
    cursor.execute(
        "INSERT INTO registry.ingest_runs_datasets (run_id, dataset_id, operation) VALUES (%s, %s, %s)",
        (run_id, dataset_id, operation),
    )
    return dataset_id


def register_file(cursor, run_id, path, dataset_id=None, source_metadata=None):
    """Record an observed file and optionally link it to a dataset."""
    path = Path(path).resolve(strict=True)
    relative_path = datalake_relative_path(path)
    stat = path.stat()
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    sha256 = digest.hexdigest()
    # Serialize allocation for a path, including the first insert.
    cursor.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (relative_path,)
    )
    cursor.execute(
        "SELECT id, content_sha256 FROM registry.files WHERE relative_path = %s AND deleted_at IS NULL FOR UPDATE",
        (relative_path,),
    )
    previous = cursor.fetchone()
    modified_at = datetime.fromtimestamp(stat.st_mtime, tz=UTC)
    media_type = mimetypes.guess_type(path.name)[0]
    if previous:
        file_id, old_sha256 = previous
        if old_sha256 is None:
            raise ValueError(
                f"Cannot determine whether previously registered file changed without a stored checksum: {relative_path}"
            )
        operation = "unchanged" if old_sha256 == sha256 else "updated"
        cursor.execute(
            """UPDATE registry.files SET size_bytes = %s, media_type = %s,
            content_sha256 = %s, filesystem_modified_at = %s, last_seen_at = now(),
            source_metadata = source_metadata || %s,
            missing_since = NULL WHERE id = %s""",
            (
                stat.st_size,
                media_type,
                sha256,
                modified_at,
                Jsonb(source_metadata or {}),
                file_id,
            ),
        )
    else:
        operation = "created"
        cursor.execute(
            """INSERT INTO registry.files (relative_path, size_bytes, media_type, content_sha256, filesystem_modified_at, source_metadata)
            VALUES (%s, %s, %s, %s, %s, %s) RETURNING id""",
            (
                relative_path,
                stat.st_size,
                media_type,
                sha256,
                modified_at,
                Jsonb(source_metadata or {}),
            ),
        )
        file_id = cursor.fetchone()[0]
    cursor.execute(
        "INSERT INTO registry.ingest_runs_files (run_id, file_id, operation) VALUES (%s, %s, %s)",
        (run_id, file_id, operation),
    )
    if dataset_id:
        cursor.execute(
            "INSERT INTO registry.datasets_files (dataset_id, file_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (dataset_id, file_id),
        )
    return file_id


class Ingest:
    """Write one connector run's files and CSV dataset to the warehouse."""

    def __init__(self, db, connector, attachment_root):
        self.db = db
        self.connector = connector
        self.attachment_root = Path(attachment_root)
        self._run = None
        self.run_id = None
        self._files = []

    def __enter__(self):
        self._run = ingest_run(self.db, self.connector)
        self.run_id = self._run.__enter__()
        return self

    def __exit__(self, *args):
        return self._run.__exit__(*args)

    def resolve_dataset(self, destination_action, table_name, existing_table_name):
        """Resolve the destination and check the configured datalake."""
        result = resolve_db_table_name(
            self.db, destination_action, table_name, existing_table_name
        )
        datalake_relative_path(self.attachment_root)
        return result

    @contextmanager
    def staging(self, dataset):
        """Create a temporary workspace on the datalake filesystem."""
        destination = self.attachment_root / dataset
        datalake_relative_path(destination)
        destination.mkdir(parents=True, exist_ok=True)
        with TemporaryDirectory(dir=destination) as directory:
            yield Path(directory)

    def write_file(self, source, name, *, metadata=None, dataset_source=False):
        """Queue a file for publication with the current dataset."""
        source = Path(source)
        self._files.append((source, name, metadata, dataset_source))
        return source

    def write_dataset(self, dataset, source, identity, record_count, timestamp_column):
        """Publish files and merge a CSV dataset, recording the result in the registry.

        The timestamp column preserves the connector's existing sidecar contract.
        """
        from psycopg import sql

        from f.connectors.csv.csv_to_postgres import transform_csv_data

        destination = self.attachment_root / dataset
        targets = [
            (Path(temp), destination / name, metadata, is_source)
            for temp, name, metadata, is_source in self._files
        ]
        if sum(temp == Path(source) for temp, _, _, _ in targets) != 1:
            raise ValueError("The CSV source must be included exactly once in files")
        if len({target for _, target, _, _ in targets}) != len(targets):
            raise ValueError("File destinations must be distinct")
        require_known_file_hashes(self.db, [target for _, target, _, _ in targets])
        source_path = None
        for temp, target, _, _ in targets:
            os.replace(temp, target)
            if temp == source:
                source_path = target
            logger.info("Saved file to %s", target)
        with connect(conninfo(self.db)) as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = %s)",
                (dataset,),
            )
            existed = cursor.fetchone()[0]
            dataset_id = None
            if record_count:
                rows = transform_csv_data(source_path, identity)
                writer = StructuredDBWriter(conninfo(self.db), dataset)
                inserted, updated = writer.handle_output(
                    rows, atomic=True, connection=connection, return_counts=True
                )
                operation = (
                    "created"
                    if not existed
                    else "updated"
                    if inserted or updated
                    else "unchanged"
                )
                dataset_id = register_dataset(cursor, self.run_id, dataset, operation)
            elif existed:
                dataset_id = register_dataset(cursor, self.run_id, dataset, "unchanged")
            for _, target, metadata, is_source in targets:
                register_file(
                    cursor,
                    self.run_id,
                    target,
                    dataset_id if is_source else None,
                    metadata,
                )
            table = sql.Identifier(f"{dataset.lower()}__metadata")
            cursor.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(table))
            cursor.execute(
                sql.SQL("CREATE TABLE {} ({} TIMESTAMPTZ NOT NULL)").format(
                    table, sql.Identifier(timestamp_column)
                )
            )
            cursor.execute(
                sql.SQL("INSERT INTO {} ({}) VALUES (now())").format(
                    table, sql.Identifier(timestamp_column)
                )
            )
            complete_run(cursor, self.run_id)
