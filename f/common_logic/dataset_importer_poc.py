"""Isolated SQL-backed engine for the Dataset Importer POC.

This module deliberately does not call the existing dataset importer or its
``StructuredDBWriter``.  That writer's upsert-on-``_id`` contract is not
compatible with append, merge, and sync semantics.
"""

import base64
import csv
import hashlib
import json
import uuid
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path

from psycopg import connect, sql

from f.common_logic.db_operations import conninfo
from f.common_logic.identifier_utils import normalize_identifier

SCHEMA = "dataset_importer_poc"
MAX_SOURCE_BYTES = 25 * 1024 * 1024
MAX_COLUMNS = 150
VALID_GOALS = {"create", "append", "merge", "sync"}
VALID_POLICIES = {"imported", "existing"}


class ImportValidationError(ValueError):
    """An import cannot safely proceed."""


def _conninfo(db):
    """Accept either a Windmill PostgreSQL resource or a test connection string."""
    return db if isinstance(db, str) else conninfo(db)


def _quoted_table(table_name):
    return sql.SQL("{}.{}").format(sql.Identifier("public"), sql.Identifier(table_name))


def _payload(uploaded_file):
    if isinstance(uploaded_file, list):
        if len(uploaded_file) != 1:
            raise ImportValidationError("Upload exactly one source file.")
        uploaded_file = uploaded_file[0]
    if not isinstance(uploaded_file, dict):
        raise ImportValidationError("The upload payload is invalid.")
    name = Path(str(uploaded_file.get("name", ""))).name
    encoded = uploaded_file.get("data")
    if not name or not isinstance(encoded, str):
        raise ImportValidationError("The upload must include a file name and data.")
    try:
        contents = base64.b64decode(encoded, validate=True)
    except ValueError as exc:
        raise ImportValidationError(
            "The uploaded file is not valid base64 data."
        ) from exc
    if len(contents) > MAX_SOURCE_BYTES:
        raise ImportValidationError("Source uploads must not exceed 25 MiB.")
    return name, contents


def _json_value(value):
    if isinstance(value, (dict, list)):
        return json.dumps(value, separators=(",", ":"), ensure_ascii=False)
    return value


def _parse_csv(contents):
    try:
        text = contents.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ImportValidationError("CSV files must be UTF-8 encoded.") from exc
    try:
        reader = csv.DictReader(StringIO(text))
        if not reader.fieldnames or any(
            name is None or not name.strip() for name in reader.fieldnames
        ):
            raise ImportValidationError("CSV files must have non-empty column names.")
        if len(set(reader.fieldnames)) != len(reader.fieldnames):
            raise ImportValidationError(
                "CSV files cannot contain duplicate column names."
            )
        # CSV has no representation for a JSON-style missing key. An empty cell is
        # an explicitly supplied empty value, which the importer stores as NULL.
        rows = [
            {key: (None if value == "" else value) for key, value in row.items()}
            for row in reader
        ]
    except csv.Error as exc:
        raise ImportValidationError("The CSV file is malformed.") from exc
    return rows


def _parse_geojson(contents):
    try:
        document = json.loads(contents)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ImportValidationError("The GeoJSON file is malformed.") from exc
    if document.get("type") != "FeatureCollection" or not isinstance(
        document.get("features"), list
    ):
        raise ImportValidationError("GeoJSON must be a FeatureCollection.")
    rows = []
    for feature in document["features"]:
        if not isinstance(feature, dict) or feature.get("type") != "Feature":
            raise ImportValidationError("GeoJSON collections must contain Features.")
        properties = feature.get("properties") or {}
        if not isinstance(properties, dict):
            raise ImportValidationError("GeoJSON feature properties must be an object.")
        row = {key: _json_value(value) for key, value in properties.items()}
        if "id" in feature:
            row["id"] = _json_value(feature["id"])
        geometry = feature.get("geometry")
        if geometry is not None:
            if (
                not isinstance(geometry, dict)
                or geometry.get("type") == "GeometryCollection"
            ):
                raise ImportValidationError("Unsupported GeoJSON geometry.")
            if "coordinates" not in geometry:
                raise ImportValidationError("GeoJSON geometries must have coordinates.")
            row["g__type"] = geometry["type"]
            row["g__coordinates"] = json.dumps(
                geometry["coordinates"], separators=(",", ":")
            )
        else:
            row["g__type"] = None
            row["g__coordinates"] = None
        rows.append(row)
    return rows


def _parse_source(name, contents):
    suffix = Path(name).suffix.lower()
    if suffix == ".csv":
        return "csv", _parse_csv(contents)
    if suffix in {".geojson", ".json"}:
        return "geojson", _parse_geojson(contents)
    raise ImportValidationError("This POC currently supports CSV and GeoJSON uploads.")


def _stored_name(source_name):
    # _id belongs to the POC's internal row identity. Preserve uploaded IDs as data.
    if source_name == "_id":
        return "source_id"
    return normalize_identifier(
        source_name,
        make_snake=False,
        ensure_leading_alpha=False,
        sep_policy="underscore",
    )


def _source_mapping(rows):
    source_fields = []
    seen = set()
    for row in rows:
        for field in row:
            if field not in seen:
                seen.add(field)
                source_fields.append(field)
    mapping = {field: _stored_name(field) for field in source_fields}
    reverse = {}
    for source, stored in mapping.items():
        if stored in reverse:
            raise ImportValidationError(
                f"Columns '{reverse[stored]}' and '{source}' have the same stored name. Rename one column."
            )
        if stored == "_id":
            raise ImportValidationError("'_id' is reserved for internal row identity.")
        reverse[stored] = source
    if len(mapping) + 1 > MAX_COLUMNS:
        raise ImportValidationError(
            "An imported dataset may contain at most 150 columns."
        )
    return mapping


def _ensure_schema(cursor):
    cursor.execute(
        sql.SQL("CREATE SCHEMA IF NOT EXISTS {};").format(sql.Identifier(SCHEMA))
    )
    cursor.execute(
        sql.SQL(
            """CREATE TABLE IF NOT EXISTS {}.import_sessions (
                import_id UUID PRIMARY KEY,
                goal TEXT NOT NULL,
                target_table TEXT NOT NULL,
                source_name TEXT NOT NULL,
                source_sha256 TEXT NOT NULL,
                source_format TEXT NOT NULL,
                source_mapping JSONB NOT NULL,
                status TEXT NOT NULL,
                preview JSONB,
                created_at TIMESTAMPTZ NOT NULL,
                applied_at TIMESTAMPTZ
            );"""
        ).format(sql.Identifier(SCHEMA))
    )
    cursor.execute(
        sql.SQL(
            """CREATE TABLE IF NOT EXISTS {}.import_rows (
                import_id UUID NOT NULL REFERENCES {}.import_sessions(import_id) ON DELETE CASCADE,
                row_ordinal INTEGER NOT NULL,
                payload JSONB NOT NULL,
                PRIMARY KEY (import_id, row_ordinal)
            );"""
        ).format(sql.Identifier(SCHEMA), sql.Identifier(SCHEMA))
    )


def list_datasets(db):
    """Return ordinary public datasets in alphabetical order."""
    with connect(_conninfo(db), autocommit=True) as conn, conn.cursor() as cursor:
        cursor.execute(
            """SELECT table_name FROM information_schema.tables
               WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
               AND table_name NOT LIKE '%\\_\\_columns' ESCAPE '\\'
               ORDER BY table_name"""
        )
        return [row[0] for row in cursor.fetchall()]


def check_dataset_name(db, dataset_name):
    table_name = normalize_identifier(dataset_name)
    with connect(_conninfo(db), autocommit=True) as conn, conn.cursor() as cursor:
        cursor.execute(
            """SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = %s
            )""",
            (table_name,),
        )
        exists = cursor.fetchone()[0]
    return {"table_name": table_name, "available": not exists}


def stage_import(db, uploaded_file, goal, target_table):
    """Parse and persist one source file for review without touching its target table."""
    if goal not in VALID_GOALS:
        raise ImportValidationError("Choose Create, Append, Merge, or Sync.")
    if not target_table:
        raise ImportValidationError("Choose or name a target dataset.")
    name, contents = _payload(uploaded_file)
    source_format, rows = _parse_source(name, contents)
    mapping = _source_mapping(rows)
    table_name = normalize_identifier(target_table)
    import_id = uuid.uuid4()
    now = datetime.now(UTC)
    with connect(_conninfo(db)) as conn, conn.cursor() as cursor:
        _ensure_schema(cursor)
        cursor.execute(
            """SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = %s
            )""",
            (table_name,),
        )
        exists = cursor.fetchone()[0]
        if goal == "create" and exists:
            raise ImportValidationError("A dataset with this name already exists.")
        if goal != "create" and not exists:
            raise ImportValidationError("Select an existing dataset for this goal.")
        cursor.execute(
            sql.SQL("""INSERT INTO {}.import_sessions
                (import_id, goal, target_table, source_name, source_sha256, source_format, source_mapping, status, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'staged', %s)""").format(
                sql.Identifier(SCHEMA)
            ),
            (
                import_id,
                goal,
                table_name,
                name,
                hashlib.sha256(contents).hexdigest(),
                source_format,
                json.dumps(mapping),
                now,
            ),
        )
        cursor.executemany(
            sql.SQL(
                "INSERT INTO {}.import_rows (import_id, row_ordinal, payload) VALUES (%s, %s, %s)"
            ).format(sql.Identifier(SCHEMA)),
            [
                (
                    import_id,
                    ordinal,
                    json.dumps({mapping[key]: value for key, value in row.items()}),
                )
                for ordinal, row in enumerate(rows, 1)
            ],
        )
    return {
        "import_id": str(import_id),
        "source_format": source_format,
        "record_count": len(rows),
        "fields": list(mapping),
    }


def _session(cursor, import_id, lock=False):
    lock_sql = " FOR UPDATE" if lock else ""
    cursor.execute(
        sql.SQL(
            "SELECT goal, target_table, source_mapping, status, preview FROM {}.import_sessions WHERE import_id = %s"
            + lock_sql
        ).format(sql.Identifier(SCHEMA)),
        (import_id,),
    )
    result = cursor.fetchone()
    if not result:
        raise ImportValidationError(
            "This import session does not exist or has expired."
        )
    return result


def _target_columns(cursor, table_name):
    cursor.execute(
        """SELECT column_name, data_type FROM information_schema.columns
           WHERE table_schema = 'public' AND table_name = %s ORDER BY ordinal_position""",
        (table_name,),
    )
    return dict(cursor.fetchall())


def _target_fingerprint(cursor, table_name):
    """Return a database-side baseline used to reject a stale confirmation."""
    cursor.execute(
        sql.SQL(
            "SELECT count(*), coalesce(sum(hashtextextended(row_to_json(t)::text, 0)::numeric), 0) FROM {} t"
        ).format(_quoted_table(table_name))
    )
    row_count, row_hash = cursor.fetchone()
    return {"row_count": row_count, "row_hash": str(row_hash)}


def _identity_stored(source_mapping, selected):
    if not selected or len(selected) > 3 or len(set(selected)) != len(selected):
        raise ImportValidationError(
            "Select one to three distinct record identity fields."
        )
    missing = [field for field in selected if field not in source_mapping]
    if missing:
        raise ImportValidationError(
            "Selected identity fields are not present in the import."
        )
    return [source_mapping[field] for field in selected]


def _identity_condition(identity_columns, staged_alias="s", target_alias="t"):
    return sql.SQL(" AND ").join(
        sql.SQL("({}.payload ->> {} IS NOT DISTINCT FROM {}.{})").format(
            sql.Identifier(staged_alias),
            sql.Literal(column),
            sql.Identifier(target_alias),
            sql.Identifier(column),
        )
        for column in identity_columns
    )


def preview_import(db, import_id, identity_fields=None, update_policy="imported"):
    """Calculate a SQL-side import plan and retain it for one confirmation."""
    if update_policy not in VALID_POLICIES:
        raise ImportValidationError("Choose an update policy.")
    with connect(_conninfo(db)) as conn, conn.cursor() as cursor:
        _ensure_schema(cursor)
        goal, table_name, mapping, status, _ = _session(cursor, import_id)
        if status == "applied":
            raise ImportValidationError("This import has already been applied.")
        stored_columns = list(mapping.values())
        target_columns = _target_columns(cursor, table_name)
        incompatible = [
            column
            for column in stored_columns
            if column in target_columns
            and target_columns[column] not in {"text", "character varying"}
        ]
        if incompatible:
            raise ImportValidationError(
                "This POC can only update existing text columns: "
                + ", ".join(incompatible)
            )
        if goal in {"merge", "sync"}:
            identity_columns = _identity_stored(mapping, identity_fields or [])
            missing_target = [
                column for column in identity_columns if column not in target_columns
            ]
            if missing_target:
                raise ImportValidationError(
                    "Selected identity fields do not exist in the target dataset."
                )
        else:
            identity_columns = []
        if (
            goal != "create"
            and len(set(target_columns) | set(stored_columns)) > MAX_COLUMNS
        ):
            raise ImportValidationError("The final dataset would exceed 150 columns.")
        if goal in {"create", "sync"}:
            cursor.execute(
                sql.SQL(
                    "SELECT count(*) FROM {}.import_rows WHERE import_id = %s"
                ).format(sql.Identifier(SCHEMA)),
                (import_id,),
            )
            if cursor.fetchone()[0] == 0:
                raise ImportValidationError("Create and Sync imports cannot be empty.")
        if identity_columns:
            identity_expr = sql.SQL(", ").join(
                sql.SQL("payload ->> {}").format(sql.Literal(column))
                for column in identity_columns
            )
            cursor.execute(
                sql.SQL(
                    "SELECT 1 FROM {}.import_rows WHERE import_id = %s GROUP BY {} HAVING count(*) > 1 LIMIT 1"
                ).format(sql.Identifier(SCHEMA), identity_expr),
                (import_id,),
            )
            if cursor.fetchone():
                raise ImportValidationError(
                    "Duplicate record identities were found in the import."
                )
            target_expr = sql.SQL(", ").join(
                sql.Identifier(column) for column in identity_columns
            )
            cursor.execute(
                sql.SQL(
                    "SELECT 1 FROM {} GROUP BY {} HAVING count(*) > 1 LIMIT 1"
                ).format(_quoted_table(table_name), target_expr)
            )
            if cursor.fetchone():
                raise ImportValidationError(
                    "Duplicate record identities were found in the target dataset."
                )

        additions = updates = deleted = unchanged = 0
        cursor.execute(
            sql.SQL("SELECT count(*) FROM {}.import_rows WHERE import_id = %s").format(
                sql.Identifier(SCHEMA)
            ),
            (import_id,),
        )
        staged_count = cursor.fetchone()[0]
        new_columns = len(set(stored_columns) - set(target_columns))
        if goal == "create":
            additions = staged_count
        elif goal == "append":
            additions = staged_count
            cursor.execute(
                sql.SQL("SELECT count(*) FROM {}").format(_quoted_table(table_name))
            )
            unchanged = cursor.fetchone()[0]
        else:
            condition = _identity_condition(identity_columns)
            cursor.execute(
                sql.SQL(
                    "SELECT count(*) FROM {}.import_rows s WHERE s.import_id = %s AND NOT EXISTS (SELECT 1 FROM {} t WHERE {})"
                ).format(sql.Identifier(SCHEMA), _quoted_table(table_name), condition),
                (import_id,),
            )
            additions = cursor.fetchone()[0]
            if update_policy == "imported":
                comparable = [
                    column
                    for column in stored_columns
                    if column in target_columns and column != "_id"
                ]
                if comparable:
                    changes = sql.SQL(" OR ").join(
                        sql.SQL(
                            "(s.payload ? {} AND (s.payload ->> {}) IS DISTINCT FROM t.{})"
                        ).format(
                            sql.Literal(column),
                            sql.Literal(column),
                            sql.Identifier(column),
                        )
                        for column in comparable
                    )
                    cursor.execute(
                        sql.SQL(
                            "SELECT count(*) FROM {}.import_rows s JOIN {} t ON {} WHERE s.import_id = %s AND ({})"
                        ).format(
                            sql.Identifier(SCHEMA),
                            _quoted_table(table_name),
                            condition,
                            changes,
                        ),
                        (import_id,),
                    )
                    updates = cursor.fetchone()[0]
            cursor.execute(
                sql.SQL(
                    "SELECT count(*) FROM {}.import_rows s JOIN {} t ON {} WHERE s.import_id = %s"
                ).format(sql.Identifier(SCHEMA), _quoted_table(table_name), condition),
                (import_id,),
            )
            matched = cursor.fetchone()[0]
            if goal == "sync":
                cursor.execute(
                    sql.SQL(
                        "SELECT count(*) FROM {} t WHERE NOT EXISTS (SELECT 1 FROM {}.import_rows s WHERE s.import_id = %s AND {})"
                    ).format(
                        _quoted_table(table_name), sql.Identifier(SCHEMA), condition
                    ),
                    (import_id,),
                )
                deleted = cursor.fetchone()[0]
            unchanged = matched - updates
            if goal == "merge":
                cursor.execute(
                    sql.SQL(
                        "SELECT count(*) FROM {} t WHERE NOT EXISTS (SELECT 1 FROM {}.import_rows s WHERE s.import_id = %s AND {})"
                    ).format(
                        _quoted_table(table_name), sql.Identifier(SCHEMA), condition
                    ),
                    (import_id,),
                )
                unchanged += cursor.fetchone()[0]
        preview = {
            "identity_fields": identity_fields or [],
            "update_policy": update_policy,
            "deleted": deleted,
            "updated": updates,
            "added": additions,
            "columns_added": new_columns,
            "unchanged": unchanged,
            "final_count": unchanged + updates + additions,
            "target_fingerprint": _target_fingerprint(cursor, table_name)
            if goal != "create"
            else None,
        }
        cursor.execute(
            sql.SQL(
                "UPDATE {}.import_sessions SET status = 'reviewed', preview = %s WHERE import_id = %s"
            ).format(sql.Identifier(SCHEMA)),
            (json.dumps(preview), import_id),
        )
    return preview


def apply_import(db, import_id):
    """Apply the reviewed plan in one transaction.  A second confirmation is rejected."""
    with connect(_conninfo(db)) as conn, conn.cursor() as cursor:
        _ensure_schema(cursor)
        goal, table_name, mapping, status, preview = _session(
            cursor, import_id, lock=True
        )
        if status != "reviewed" or not preview:
            raise ImportValidationError("Review this import before confirming it.")
        stored_columns = list(mapping.values())
        target_columns = _target_columns(cursor, table_name)
        if goal == "create":
            cursor.execute(
                sql.SQL("CREATE TABLE {} (_id TEXT PRIMARY KEY)").format(
                    _quoted_table(table_name)
                )
            )
            target_columns = {"_id": "text"}
        else:
            cursor.execute(
                sql.SQL("LOCK TABLE {} IN SHARE ROW EXCLUSIVE MODE").format(
                    _quoted_table(table_name)
                )
            )
            if _target_fingerprint(cursor, table_name) != preview["target_fingerprint"]:
                raise ImportValidationError(
                    "The target dataset changed after review. Review the import again."
                )
        for column in stored_columns:
            if column not in target_columns:
                cursor.execute(
                    sql.SQL("ALTER TABLE {} ADD COLUMN {} TEXT").format(
                        _quoted_table(table_name), sql.Identifier(column)
                    )
                )
                target_columns[column] = "text"
        columns = [column for column in stored_columns if column in target_columns]
        insert_columns = ["_id", *columns]
        insert_values = sql.SQL(", ").join(
            [
                sql.SQL("gen_random_uuid()::text"),
                *[
                    sql.SQL("s.payload ->> {}").format(sql.Literal(column))
                    for column in columns
                ],
            ]
        )
        # pgcrypto may not be installed. UUID generation in the application keeps this portable.
        insert_values = sql.SQL(", ").join(
            [
                sql.SQL("md5(s.import_id::text || ':' || s.row_ordinal::text)"),
                *[
                    sql.SQL("s.payload ->> {}").format(sql.Literal(column))
                    for column in columns
                ],
            ]
        )
        insert_statement = sql.SQL(
            "INSERT INTO {} ({}) SELECT {} FROM {}.import_rows s WHERE s.import_id = %s"
        ).format(
            _quoted_table(table_name),
            sql.SQL(", ").join(map(sql.Identifier, insert_columns)),
            insert_values,
            sql.Identifier(SCHEMA),
        )
        identity_columns = [mapping[field] for field in preview["identity_fields"]]
        if goal in {"create", "append"}:
            cursor.execute(insert_statement, (import_id,))
        else:
            condition = _identity_condition(identity_columns)
            if preview["update_policy"] == "imported":
                assignments = sql.SQL(", ").join(
                    sql.SQL(
                        "{} = CASE WHEN s.payload ? {} THEN s.payload ->> {} ELSE t.{} END"
                    ).format(
                        sql.Identifier(column),
                        sql.Literal(column),
                        sql.Literal(column),
                        sql.Identifier(column),
                    )
                    for column in columns
                    if column != "_id"
                )
                if assignments:
                    cursor.execute(
                        sql.SQL(
                            "UPDATE {} t SET {} FROM {}.import_rows s WHERE s.import_id = %s AND {}"
                        ).format(
                            _quoted_table(table_name),
                            assignments,
                            sql.Identifier(SCHEMA),
                            condition,
                        ),
                        (import_id,),
                    )
            cursor.execute(
                sql.SQL(
                    "INSERT INTO {} ({}) SELECT {} FROM {}.import_rows s WHERE s.import_id = %s AND NOT EXISTS (SELECT 1 FROM {} t WHERE {})"
                ).format(
                    _quoted_table(table_name),
                    sql.SQL(", ").join(map(sql.Identifier, insert_columns)),
                    insert_values,
                    sql.Identifier(SCHEMA),
                    _quoted_table(table_name),
                    condition,
                ),
                (import_id,),
            )
            if goal == "sync":
                cursor.execute(
                    sql.SQL(
                        "DELETE FROM {} t WHERE NOT EXISTS (SELECT 1 FROM {}.import_rows s WHERE s.import_id = %s AND {})"
                    ).format(
                        _quoted_table(table_name), sql.Identifier(SCHEMA), condition
                    ),
                    (import_id,),
                )
        cursor.execute(
            sql.SQL(
                "UPDATE {}.import_sessions SET status = 'applied', applied_at = %s WHERE import_id = %s"
            ).format(sql.Identifier(SCHEMA)),
            (datetime.now(UTC), import_id),
        )
    return {"success": True, "preview": preview}
