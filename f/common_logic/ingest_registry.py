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

from psycopg import connect
from psycopg.types.json import Jsonb

from f.common_logic.db_operations import conninfo

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
