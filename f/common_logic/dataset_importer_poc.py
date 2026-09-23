"""Isolated SQL-backed engine for the Dataset Importer POC.

This module deliberately does not call the existing dataset importer or its
``StructuredDBWriter``.  That writer's upsert-on-``_id`` contract is not
compatible with append, merge, and sync semantics.
"""

import base64
import csv
import hashlib
import json
import os
import stat
import uuid
import zipfile
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from io import BytesIO, StringIO
from pathlib import Path
from tempfile import TemporaryDirectory

from psycopg import connect, errors, sql

from f.common_logic.db_operations import conninfo
from f.common_logic.identifier_utils import normalize_identifier

SCHEMA = "dataset_importer_poc"
MAX_SOURCE_BYTES = 25 * 1024 * 1024
MAX_EXPANDED_BYTES = 100 * 1024 * 1024
MAX_COLUMNS = 150
MAX_ARCHIVE_MEMBERS = 1000
SESSION_TTL = timedelta(hours=24)
VALID_GOALS = {"create", "append", "merge", "sync"}
VALID_POLICIES = {"imported", "existing"}
AUXILIARY_SUFFIXES = ("__columns", "__labels", "__metadata")
SHAPEFILE_EXTENSIONS = {".shp", ".shx", ".dbf", ".prj", ".cpg"}
SUPPORTED_EXTENSIONS = {
    ".csv",
    ".geojson",
    ".gpx",
    ".gpkg",
    ".json",
    ".kml",
    ".xls",
    ".xlsx",
    ".xml",
}


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
    max_encoded_bytes = 4 * ((MAX_SOURCE_BYTES + 2) // 3)
    if len(encoded) > max_encoded_bytes:
        raise ImportValidationError("Source uploads must not exceed 25 MiB.")
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
        rows = []
        for row in reader:
            if None in row or any(value is None for value in row.values()):
                raise ImportValidationError("CSV rows must match the header column count.")
            rows.append(
                {key: (None if value == "" else value) for key, value in row.items()}
            )
    except csv.Error as exc:
        raise ImportValidationError("The CSV file is malformed.") from exc
    return rows


def _geojson_rows(document):
    if not isinstance(document, dict):
        raise ImportValidationError("GeoJSON must be a FeatureCollection.")
    if document.get("type") != "FeatureCollection" or not isinstance(
        document.get("features"), list
    ):
        raise ImportValidationError("GeoJSON must be a FeatureCollection.")
    rows = []
    for feature in document["features"]:
        if not isinstance(feature, dict) or feature.get("type") != "Feature":
            raise ImportValidationError("GeoJSON collections must contain Features.")
        properties = feature.get("properties")
        if properties is None:
            properties = {}
        if not isinstance(properties, dict):
            raise ImportValidationError("GeoJSON feature properties must be an object.")
        row = {key: _json_value(value) for key, value in properties.items()}
        reserved = {"feature.id", "g__type", "g__coordinates"} & row.keys()
        if reserved:
            raise ImportValidationError(
                "GeoJSON properties use reserved fields: " + ", ".join(sorted(reserved))
            )
        if "id" in feature:
            row["feature.id"] = _json_value(feature["id"])
        geometry = feature.get("geometry")
        if geometry is not None:
            if not isinstance(geometry, dict) or not isinstance(
                geometry.get("type"), str
            ):
                raise ImportValidationError("Unsupported GeoJSON geometry.")
            if geometry["type"] == "GeometryCollection":
                spatial_data = geometry.get("geometries")
                if not isinstance(spatial_data, list) or not all(
                    isinstance(item, dict) for item in spatial_data
                ):
                    raise ImportValidationError("Unsupported GeoJSON geometry.")
            else:
                spatial_data = geometry.get("coordinates")
                if not isinstance(spatial_data, (list, tuple)):
                    raise ImportValidationError("Unsupported GeoJSON geometry.")
            row["g__type"] = geometry.get("type")
            row["g__coordinates"] = json.dumps(
                spatial_data, separators=(",", ":")
            )
        else:
            row["g__type"] = None
            row["g__coordinates"] = None
        rows.append(row)
    return rows


def _parse_geojson(contents):
    try:
        document = json.loads(contents)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ImportValidationError("The GeoJSON file is malformed.") from exc
    return _geojson_rows(document)


def _parse_json(contents):
    try:
        document = json.loads(contents)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ImportValidationError("The JSON file is malformed.") from exc
    if not isinstance(document, list) or not document:
        raise ImportValidationError("JSON must be a non-empty array of objects.")
    if not all(isinstance(row, dict) for row in document):
        raise ImportValidationError("Every JSON record must be an object.")
    return [
        {key: _json_value(value) for key, value in row.items()} for row in document
    ]


def _tabular_rows(data):
    if not data:
        return []
    headers = data[0]
    if not headers or any(not isinstance(header, str) or not header.strip() for header in headers):
        raise ImportValidationError("Tabular files must have non-empty column names.")
    if len(headers) != len(set(headers)):
        raise ImportValidationError("Tabular files cannot contain duplicate column names.")
    rows = []
    for values in data[1:]:
        if len(values) != len(headers):
            raise ImportValidationError("A tabular row has the wrong number of values.")
        rows.append(
            {
                header: None if value == "" else _json_value(value)
                for header, value in zip(headers, values)
            }
        )
    return rows


def _parse_converted_paths(file_paths):
    import fiona

    from f.common_logic.data_conversion import convert_data, detect_structured_data_type

    detected = detect_structured_data_type([str(path) for path in file_paths])
    if detected == "unsupported" or detected == "xml":
        raise ImportValidationError("The uploaded file type is not supported.")
    if detected == "geopackage":
        spatial_layers = []
        for layer in fiona.listlayers(file_paths[0]):
            with fiona.open(file_paths[0], layer=layer) as collection:
                if collection.schema["geometry"] not in (None, "None"):
                    spatial_layers.append(layer)
        if len(spatial_layers) != 1:
            raise ImportValidationError(
                "A GeoPackage must contain exactly one spatial layer."
            )
    try:
        converted, output_format = convert_data(
            [str(path) for path in file_paths], detected
        )
    except (OSError, ValueError) as exc:
        raise ImportValidationError(str(exc)) from exc
    rows = _tabular_rows(converted) if output_format == "csv" else _geojson_rows(converted)
    return detected, rows


def _safe_archive_members(contents):
    try:
        archive = zipfile.ZipFile(BytesIO(contents))
    except zipfile.BadZipFile as exc:
        raise ImportValidationError("The ZIP file is malformed.") from exc
    members = [member for member in archive.infolist() if not member.is_dir()]
    if not members:
        raise ImportValidationError("The ZIP file is empty.")
    if len(members) > MAX_ARCHIVE_MEMBERS:
        raise ImportValidationError("The ZIP file contains too many files.")
    if sum(member.file_size for member in members) > MAX_EXPANDED_BYTES:
        raise ImportValidationError("ZIP contents must not exceed 100 MiB uncompressed.")
    seen = set()
    for member in members:
        member_path = Path(member.filename)
        mode = member.external_attr >> 16
        if (
            member_path.is_absolute()
            or ".." in member_path.parts
            or stat.S_ISLNK(mode)
            or member.filename in seen
        ):
            raise ImportValidationError("The ZIP file contains an unsafe file path.")
        seen.add(member.filename)
    return archive, members


def _extract_member(archive, member, destination):
    target = destination / Path(member.filename).name
    if target.exists():
        raise ImportValidationError("ZIP files cannot contain duplicate file names.")
    with archive.open(member) as source, target.open("wb") as output:
        output.write(source.read())
    return target


def _parse_zip(contents):
    archive, members = _safe_archive_members(contents)
    with archive, TemporaryDirectory() as directory:
        destination = Path(directory)
        suffixes = [Path(member.filename).suffix.lower() for member in members]
        if ".shp" in suffixes:
            if any(suffix not in SHAPEFILE_EXTENSIONS for suffix in suffixes):
                raise ImportValidationError(
                    "A Shapefile ZIP cannot contain unrelated files."
                )
            stems = {Path(member.filename).stem for member in members}
            if len(stems) != 1 or not {".shp", ".shx", ".dbf"}.issubset(suffixes):
                raise ImportValidationError(
                    "A Shapefile ZIP must contain matching .shp, .shx, and .dbf files."
                )
            paths = [_extract_member(archive, member, destination) for member in members]
            _, rows = _parse_converted_paths(paths)
            return "shapefile", rows
        if any(suffix not in SUPPORTED_EXTENSIONS for suffix in suffixes):
            raise ImportValidationError("The ZIP file contains an unsupported file type.")
        rows = []
        for member in members:
            path = _extract_member(archive, member, destination)
            _, member_rows = _parse_path(path)
            rows.extend(member_rows)
        return "zip", rows


def _parse_path(path):
    suffix = path.suffix.lower()
    if suffix == ".xlsx":
        try:
            with zipfile.ZipFile(path) as workbook:
                if sum(member.file_size for member in workbook.infolist()) > MAX_EXPANDED_BYTES:
                    raise ImportValidationError(
                        "Spreadsheet contents must not exceed 100 MiB uncompressed."
                    )
        except zipfile.BadZipFile as exc:
            raise ImportValidationError("The XLSX file is malformed.") from exc
    contents = path.read_bytes()
    if suffix == ".csv":
        return "csv", _parse_csv(contents)
    if suffix == ".geojson":
        return "geojson", _parse_geojson(contents)
    if suffix == ".json":
        try:
            document = json.loads(contents)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ImportValidationError("The JSON file is malformed.") from exc
        if isinstance(document, dict) and document.get("type") == "FeatureCollection":
            return "geojson", _geojson_rows(document)
        from f.common_logic.data_conversion import detect_structured_data_type

        detected = detect_structured_data_type([str(path)])
        if detected == "cybertracker":
            return _parse_converted_paths([path])
        return "json", _parse_json(contents)
    return _parse_converted_paths([path])


def _parse_source(name, contents):
    suffix = Path(name).suffix.lower()
    if suffix == ".zip":
        return _parse_zip(contents)
    if suffix not in SUPPORTED_EXTENSIONS:
        raise ImportValidationError("The uploaded file type is not supported.")
    with TemporaryDirectory() as directory:
        path = Path(directory) / Path(name).name
        path.write_bytes(contents)
        return _parse_path(path)


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


def _source_fields(rows):
    source_fields = []
    seen = set()
    for row in rows:
        for field in row:
            if field not in seen:
                seen.add(field)
                source_fields.append(field)
    return source_fields


def _source_mapping(rows, existing_mapping=None):
    existing_mapping = existing_mapping or {}
    mapping = {
        field: existing_mapping.get(field, _stored_name(field))
        for field in _source_fields(rows)
    }
    reverse = {}
    for source, stored in existing_mapping.items():
        if stored in reverse and reverse[stored] != source:
            raise ImportValidationError(
                f"The target dataset maps both '{reverse[stored]}' and '{source}' to '{stored}'."
            )
        reverse[stored] = source
    for source, stored in mapping.items():
        if stored in reverse and reverse[stored] != source:
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
                source_data BYTEA,
                source_format TEXT NOT NULL,
                source_mapping JSONB NOT NULL,
                status TEXT NOT NULL,
                preview JSONB,
                preview_id UUID,
                created_at TIMESTAMPTZ NOT NULL,
                expires_at TIMESTAMPTZ NOT NULL,
                archive_path TEXT,
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
    cursor.execute(
        sql.SQL(
            """CREATE TABLE IF NOT EXISTS {}.dataset_registry (
                target_table TEXT PRIMARY KEY,
                columns_table TEXT UNIQUE NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('reserved', 'active')),
                source_import_id UUID UNIQUE
            )"""
        ).format(sql.Identifier(SCHEMA))
    )
    cursor.execute(
        sql.SQL(
            "ALTER TABLE {}.import_sessions ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ"
        ).format(sql.Identifier(SCHEMA))
    )
    cursor.execute(
        sql.SQL(
            "ALTER TABLE {}.import_sessions ADD COLUMN IF NOT EXISTS source_data BYTEA"
        ).format(sql.Identifier(SCHEMA))
    )
    cursor.execute(
        sql.SQL(
            "ALTER TABLE {}.import_sessions ADD COLUMN IF NOT EXISTS archive_path TEXT"
        ).format(sql.Identifier(SCHEMA))
    )
    cursor.execute(
        sql.SQL(
            "ALTER TABLE {}.import_sessions ADD COLUMN IF NOT EXISTS preview_id UUID"
        ).format(sql.Identifier(SCHEMA))
    )
    cursor.execute(
        sql.SQL(
            "UPDATE {}.import_sessions SET expires_at = created_at + INTERVAL '24 hours' WHERE expires_at IS NULL"
        ).format(sql.Identifier(SCHEMA))
    )
    cursor.execute(
        sql.SQL(
            "ALTER TABLE {}.import_sessions ALTER COLUMN expires_at SET NOT NULL"
        ).format(sql.Identifier(SCHEMA))
    )
    _cleanup_expired(cursor, datetime.now(UTC))
    _backfill_dataset_registry(cursor)


def _columns_table_name(table_name):
    return f"{table_name[:54]}__columns"


def _relation_exists(cursor, table_name):
    cursor.execute(
        """SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = %s
            AND table_type = 'BASE TABLE'
        )""",
        (table_name,),
    )
    return cursor.fetchone()[0]


def _public_relation_exists(cursor, table_name):
    cursor.execute("SELECT to_regclass(%s) IS NOT NULL", (f"public.{table_name}",))
    return cursor.fetchone()[0]


def _normalize_dataset_name(dataset_name, *, allow_auxiliary=False):
    if not isinstance(dataset_name, str) or not dataset_name.strip():
        raise ImportValidationError("Enter a dataset name.")
    table_name = normalize_identifier(dataset_name)
    if table_name == "_":
        raise ImportValidationError("Enter a dataset name containing letters or numbers.")
    if not allow_auxiliary and table_name.endswith(AUXILIARY_SUFFIXES):
        raise ImportValidationError("Dataset names cannot use a reserved suffix.")
    return table_name


def _mapping_name_is_available(cursor, table_name):
    cursor.execute(
        """SELECT table_name FROM information_schema.tables
           WHERE table_schema = 'public' AND table_type = 'BASE TABLE'"""
    )
    public_tables = [row[0] for row in cursor.fetchall()]
    mapping_name = _columns_table_name(table_name)
    if _public_relation_exists(cursor, mapping_name):
        return False
    cursor.execute("SELECT to_regclass(%s)", (f"{SCHEMA}.dataset_registry",))
    if cursor.fetchone()[0] is not None:
        cursor.execute(
            sql.SQL(
                "SELECT 1 FROM {}.dataset_registry WHERE target_table = %s OR columns_table = %s"
            ).format(sql.Identifier(SCHEMA)),
            (table_name, mapping_name),
        )
        if cursor.fetchone():
            return False
    return not any(
        existing != table_name and _columns_table_name(existing) == mapping_name
        for existing in public_tables
        if not existing.endswith(AUXILIARY_SUFFIXES)
    )


def _claim_dataset_mapping(cursor, table_name, import_id, creating):
    columns_table = _columns_table_name(table_name)
    cursor.execute(
        sql.SQL(
            "SELECT target_table, columns_table, status, source_import_id FROM {}.dataset_registry WHERE target_table = %s OR columns_table = %s FOR UPDATE"
        ).format(sql.Identifier(SCHEMA)),
        (table_name, columns_table),
    )
    claimed = cursor.fetchone()
    if claimed:
        if claimed[0] != table_name or claimed[1] != columns_table:
            raise ImportValidationError(
                "This dataset name conflicts with an existing dataset. Choose another name."
            )
        if creating and claimed[3] != import_id:
            raise ImportValidationError("A dataset with this name already exists.")
        return
    if creating and _public_relation_exists(cursor, columns_table):
        raise ImportValidationError(
            "This dataset name conflicts with an existing column mapping. Choose another name."
        )
    try:
        cursor.execute(
            sql.SQL(
                "INSERT INTO {}.dataset_registry (target_table, columns_table, status, source_import_id) VALUES (%s, %s, %s, %s)"
            ).format(sql.Identifier(SCHEMA)),
            (
                table_name,
                columns_table,
                "reserved" if creating else "active",
                import_id if creating else None,
            ),
        )
    except errors.UniqueViolation as exc:
        raise ImportValidationError(
            "This dataset name conflicts with an existing dataset. Choose another name."
        ) from exc


def _backfill_dataset_registry(cursor):
    cursor.execute(
        sql.SQL(
            "SELECT import_id, target_table, status FROM {}.import_sessions WHERE goal = 'create' AND status != 'invalidated' ORDER BY (status = 'applied') DESC, created_at DESC"
        ).format(sql.Identifier(SCHEMA))
    )
    for import_id, table_name, status in cursor.fetchall():
        active = status in {"applied", "archived"}
        if not active and _public_relation_exists(cursor, table_name):
            cursor.execute(
                sql.SQL(
                    "UPDATE {}.import_sessions SET status = 'invalidated' WHERE import_id = %s"
                ).format(sql.Identifier(SCHEMA)),
                (import_id,),
            )
            continue
        try:
            _claim_dataset_mapping(cursor, table_name, import_id, not active)
        except ImportValidationError:
            cursor.execute(
                sql.SQL(
                    "UPDATE {}.import_sessions SET status = 'invalidated' WHERE import_id = %s"
                ).format(sql.Identifier(SCHEMA)),
                (import_id,),
            )
            continue
        if active:
            cursor.execute(
                sql.SQL(
                    "UPDATE {}.dataset_registry SET status = 'active', source_import_id = NULL WHERE target_table = %s"
                ).format(sql.Identifier(SCHEMA)),
                (table_name,),
            )


def _cleanup_expired(cursor, now):
    cursor.execute(
        sql.SQL(
            "DELETE FROM {}.import_sessions WHERE expires_at <= %s AND status != 'applied' RETURNING import_id"
        ).format(sql.Identifier(SCHEMA)),
        (now,),
    )
    expired_ids = [row[0] for row in cursor.fetchall()]
    if expired_ids:
        cursor.execute(
            sql.SQL(
                "DELETE FROM {}.dataset_registry WHERE status = 'reserved' AND source_import_id = ANY(%s)"
            ).format(sql.Identifier(SCHEMA)),
            (expired_ids,),
        )
    return len(expired_ids)


def _dataset_is_eligible(cursor, table_name):
    if table_name.endswith(AUXILIARY_SUFFIXES):
        return False
    cursor.execute(
        """SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns c
            JOIN pg_catalog.pg_class t ON t.relname = c.table_name
            JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
            WHERE c.table_schema = 'public' AND c.table_name = %s
            AND c.column_name = '_id' AND c.data_type IN ('text', 'character varying')
            AND n.nspname = 'public' AND t.relkind = 'r'
            AND EXISTS (
                SELECT 1
                FROM pg_catalog.pg_constraint con
                WHERE con.conrelid = t.oid AND con.contype = 'p'
                AND con.conkey = ARRAY[c.ordinal_position::smallint]
            )
        )""",
        (table_name,),
    )
    return cursor.fetchone()[0]


def _dataset_mapping(cursor, table_name):
    target_columns = _target_columns(cursor, table_name)
    mapping = {}
    columns_table = _columns_table_name(table_name)
    if _relation_exists(cursor, columns_table):
        cursor.execute(
            sql.SQL(
                "SELECT original_column, sql_column FROM {} WHERE original_column IS NOT NULL"
            ).format(_quoted_table(columns_table))
        )
        for original, stored in cursor.fetchall():
            if original in mapping and mapping[original] != stored:
                raise ImportValidationError(
                    f"The target dataset has conflicting mappings for '{original}'."
                )
            mapping[original] = stored
    mapped_columns = set(mapping.values())
    for column in target_columns:
        if (
            column != "_id"
            and column not in mapped_columns
            and column not in mapping
        ):
            mapping[column] = column
    return mapping


def _ensure_dataset_mapping(cursor, table_name, mapping):
    columns_table = _columns_table_name(table_name)
    cursor.execute(
        sql.SQL(
            """CREATE TABLE IF NOT EXISTS {} (
                original_column TEXT PRIMARY KEY,
                sql_column TEXT UNIQUE NOT NULL
            )"""
        ).format(_quoted_table(columns_table))
    )
    cursor.execute(
        sql.SQL("LOCK TABLE {} IN SHARE ROW EXCLUSIVE MODE").format(
            _quoted_table(columns_table)
        )
    )
    for original, stored in mapping.items():
        cursor.execute(
            sql.SQL(
                "SELECT original_column, sql_column FROM {} WHERE original_column = %s OR sql_column = %s"
            ).format(_quoted_table(columns_table)),
            (original, stored),
        )
        conflicts = [row for row in cursor.fetchall() if row != (original, stored)]
        if conflicts:
            raise ImportValidationError(
                "The dataset column mapping changed after this import was staged. Stage the import again."
            )
        cursor.execute(
            sql.SQL(
                """INSERT INTO {} (original_column, sql_column)
                SELECT %s, %s WHERE NOT EXISTS (
                    SELECT 1 FROM {} WHERE original_column = %s OR sql_column = %s
                )"""
            ).format(_quoted_table(columns_table), _quoted_table(columns_table)),
            (original, stored, original, stored),
        )


def list_datasets(db):
    """Return compatible public datasets in alphabetical order."""
    with connect(_conninfo(db), autocommit=True) as conn, conn.cursor() as cursor:
        cursor.execute(
            """SELECT table_name FROM information_schema.tables
               WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
               ORDER BY table_name"""
        )
        table_names = [row[0] for row in cursor.fetchall()]
        return [
            table_name
            for table_name in table_names
            if _dataset_is_eligible(cursor, table_name)
        ]


def check_dataset_name(db, dataset_name):
    table_name = _normalize_dataset_name(dataset_name)
    with connect(_conninfo(db), autocommit=True) as conn, conn.cursor() as cursor:
        exists = _public_relation_exists(cursor, table_name)
        mapping_available = _mapping_name_is_available(cursor, table_name)
    return {
        "table_name": table_name,
        "available": not exists and mapping_available,
    }


def stage_import(db, uploaded_file, goal, target_table):
    """Parse and persist one source file for review without touching its target table."""
    if goal not in VALID_GOALS:
        raise ImportValidationError("Choose Create, Append, Merge, or Sync.")
    if not target_table:
        raise ImportValidationError("Choose or name a target dataset.")
    name, contents = _payload(uploaded_file)
    source_format, rows = _parse_source(name, contents)
    table_name = _normalize_dataset_name(
        target_table, allow_auxiliary=goal != "create"
    )
    import_id = uuid.uuid4()
    now = datetime.now(UTC)
    with connect(_conninfo(db)) as conn, conn.cursor() as cursor:
        _ensure_schema(cursor)
        _cleanup_expired(cursor, now)
        exists = _public_relation_exists(cursor, table_name)
        if goal == "create" and exists:
            raise ImportValidationError("A dataset with this name already exists.")
        if goal == "create" and not _mapping_name_is_available(cursor, table_name):
            raise ImportValidationError(
                "This dataset name conflicts with an existing dataset. Choose another name."
            )
        if goal != "create" and not exists:
            raise ImportValidationError("Select an existing dataset for this goal.")
        if goal != "create" and not _dataset_is_eligible(cursor, table_name):
            raise ImportValidationError(
                "The selected table is not a compatible Guardian Connector dataset."
            )
        _claim_dataset_mapping(cursor, table_name, import_id, goal == "create")
        existing_mapping = _dataset_mapping(cursor, table_name) if exists else {}
        mapping = _source_mapping(rows, existing_mapping)
        cursor.execute(
            sql.SQL("""INSERT INTO {}.import_sessions
                (import_id, goal, target_table, source_name, source_sha256, source_data, source_format, source_mapping, status, created_at, expires_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'staged', %s, %s)""").format(
                sql.Identifier(SCHEMA)
            ),
            (
                import_id,
                goal,
                table_name,
                name,
                hashlib.sha256(contents).hexdigest(),
                contents,
                source_format,
                json.dumps(mapping),
                now,
                now + SESSION_TTL,
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
            "SELECT goal, target_table, source_mapping, status, preview, expires_at, preview_id FROM {}.import_sessions WHERE import_id = %s"
            + lock_sql
        ).format(sql.Identifier(SCHEMA)),
        (import_id,),
    )
    result = cursor.fetchone()
    if not result:
        raise ImportValidationError(
            "This import session does not exist or has expired."
        )
    if result[5] <= datetime.now(UTC):
        raise ImportValidationError("This import session has expired. Start a new import.")
    if result[3] == "invalidated":
        raise ImportValidationError(
            "This import session conflicts with another dataset. Start a new import."
        )
    return (*result[:5], result[6])


def cleanup_expired_imports(db):
    """Delete expired staged data and return the number of removed sessions."""
    with connect(_conninfo(db)) as conn, conn.cursor() as cursor:
        _ensure_schema(cursor)
        return _cleanup_expired(cursor, datetime.now(UTC))


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
    cursor.execute(
        """SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod),
                  a.attnotnull, coalesce(pg_get_expr(d.adbin, d.adrelid), ''),
                  a.attidentity, a.attgenerated
           FROM pg_catalog.pg_attribute a
           JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
           JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
           LEFT JOIN pg_catalog.pg_attrdef d
             ON d.adrelid = a.attrelid AND d.adnum = a.attnum
           WHERE n.nspname = 'public' AND c.relname = %s
             AND a.attnum > 0 AND NOT a.attisdropped
           ORDER BY a.attnum""",
        (table_name,),
    )
    columns = [list(column) for column in cursor.fetchall()]
    cursor.execute(
        """SELECT conname, pg_get_constraintdef(oid, true)
           FROM pg_catalog.pg_constraint
           WHERE conrelid = to_regclass(%s)
           ORDER BY conname""",
        (f"public.{table_name}",),
    )
    constraints = [list(constraint) for constraint in cursor.fetchall()]
    cursor.execute(
        """SELECT indexname, indexdef FROM pg_catalog.pg_indexes
           WHERE schemaname = 'public' AND tablename = %s
           ORDER BY indexname""",
        (table_name,),
    )
    indexes = [list(index) for index in cursor.fetchall()]
    cursor.execute(
        """SELECT tgname, pg_get_triggerdef(oid, true)
           FROM pg_catalog.pg_trigger
           WHERE tgrelid = to_regclass(%s) AND NOT tgisinternal
           ORDER BY tgname""",
        (f"public.{table_name}",),
    )
    triggers = [list(trigger) for trigger in cursor.fetchall()]
    cursor.execute(
        """SELECT relrowsecurity, relforcerowsecurity
           FROM pg_catalog.pg_class
           WHERE oid = to_regclass(%s)""",
        (f"public.{table_name}",),
    )
    row_security = list(cursor.fetchone())
    cursor.execute(
        """SELECT policyname, permissive, roles::text, cmd, qual, with_check
           FROM pg_catalog.pg_policies
           WHERE schemaname = 'public' AND tablename = %s
           ORDER BY policyname""",
        (table_name,),
    )
    policies = [list(policy) for policy in cursor.fetchall()]
    return {
        "row_count": row_count,
        "row_hash": str(row_hash),
        "columns": columns,
        "constraints": constraints,
        "indexes": indexes,
        "triggers": triggers,
        "row_security": row_security,
        "policies": policies,
    }


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
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        _ensure_schema(cursor)
        goal, table_name, mapping, status, _, _ = _session(cursor, import_id)
        if status in {"applied", "archived"}:
            raise ImportValidationError("This import has already been applied.")
        stored_columns = list(mapping.values())
        target_columns = _target_columns(cursor, table_name)
        cursor.execute(
            sql.SQL("SELECT count(*) FROM {}.import_rows WHERE import_id = %s").format(
                sql.Identifier(SCHEMA)
            ),
            (import_id,),
        )
        staged_count = cursor.fetchone()[0]
        incompatible = [
            column
            for column in stored_columns
            if column in target_columns
            and target_columns[column] not in {"text", "character varying"}
        ]
        if incompatible and staged_count > 0:
            raise ImportValidationError(
                "This POC can only update existing text columns: "
                + ", ".join(incompatible)
            )
        if goal in {"merge", "sync"} and staged_count > 0:
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
            staged_count > 0
            and goal != "create"
            and len(set(target_columns) | set(stored_columns)) > MAX_COLUMNS
        ):
            raise ImportValidationError("The final dataset would exceed 150 columns.")
        if goal in {"create", "sync"} and staged_count == 0:
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
        new_columns = (
            len(set(stored_columns) - set(target_columns)) if staged_count > 0 else 0
        )
        if goal == "create":
            additions = staged_count
        elif goal == "append":
            additions = staged_count
            cursor.execute(
                sql.SQL("SELECT count(*) FROM {}").format(_quoted_table(table_name))
            )
            unchanged = cursor.fetchone()[0]
        elif staged_count == 0:
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
                comparable = [column for column in stored_columns if column != "_id"]
                if comparable:
                    changes = sql.SQL(" OR ").join(
                        (
                            sql.SQL(
                                "(s.payload ? {} AND (s.payload ->> {}) IS DISTINCT FROM t.{})"
                            ).format(
                                sql.Literal(column),
                                sql.Literal(column),
                                sql.Identifier(column),
                            )
                            if column in target_columns
                            else sql.SQL(
                                "(s.payload ? {} AND (s.payload ->> {}) IS NOT NULL)"
                            ).format(sql.Literal(column), sql.Literal(column))
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
        preview_id = uuid.uuid4()
        preview = {
            "preview_id": str(preview_id),
            "source_count": staged_count,
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
                "UPDATE {}.import_sessions SET status = 'reviewed', preview = %s, preview_id = %s WHERE import_id = %s"
            ).format(sql.Identifier(SCHEMA)),
            (json.dumps(preview), preview_id, import_id),
        )
    return preview


def _write_source_archive(table_name, source_name, source_data, import_id):
    if source_data is None:
        raise ImportValidationError(
            "This legacy import has no retained source file. Stage it again."
        )
    root = Path(
        os.environ.get("DATASET_IMPORTER_DATALAKE_ROOT", "/persistent-storage/datalake")
    )
    dataset_directory = root / table_name
    dataset_directory.mkdir(parents=True, exist_ok=True)
    destination = dataset_directory / f"{import_id}_{Path(source_name).name}"
    if destination.exists():
        if destination.read_bytes() != source_data:
            raise ImportValidationError(
                "The archive destination already contains different source data."
            )
        return str(destination), False
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    try:
        with temporary.open("xb") as output:
            output.write(source_data)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
    return str(destination), True


def _archive_applied_source(db, import_id):
    """Finish archival for sessions applied by an older importer version."""
    with connect(_conninfo(db)) as conn, conn.cursor() as cursor:
        cursor.execute(
            sql.SQL(
                "SELECT target_table, source_name, source_data, status, archive_path FROM {}.import_sessions WHERE import_id = %s FOR UPDATE"
            ).format(sql.Identifier(SCHEMA)),
            (import_id,),
        )
        session = cursor.fetchone()
        if not session:
            raise ImportValidationError("This import session does not exist.")
        table_name, source_name, source_data, status, archive_path = session
        if status == "archived":
            return archive_path
        if status != "applied":
            raise ImportValidationError(
                "Apply this import before archiving its source."
            )
        archive_path, _ = _write_source_archive(
            table_name, source_name, source_data, import_id
        )
        cursor.execute(
            sql.SQL(
                "UPDATE {}.import_sessions SET status = 'archived', archive_path = %s, source_data = NULL WHERE import_id = %s"
            ).format(sql.Identifier(SCHEMA)),
            (archive_path, import_id),
        )
    return archive_path


def apply_import(db, import_id, preview_id):
    """Archive the source and apply the reviewed plan without partial target writes."""
    with (
        connect(_conninfo(db), autocommit=True) as status_conn,
        status_conn.cursor() as status_cursor,
    ):
        _ensure_schema(status_cursor)
        status_cursor.execute(
            sql.SQL(
                "SELECT status, preview, preview_id, archive_path FROM {}.import_sessions WHERE import_id = %s"
            ).format(sql.Identifier(SCHEMA)),
            (import_id,),
        )
        existing = status_cursor.fetchone()
    if existing and existing[0] == "applied":
        if str(existing[2]) != str(preview_id):
            raise ImportValidationError(
                "This preview is no longer current. Review again."
            )
        archive_path = _archive_applied_source(db, import_id)
        return {"success": True, "preview": existing[1], "archive_path": archive_path}
    if existing and existing[0] == "archived":
        if str(existing[2]) != str(preview_id):
            raise ImportValidationError(
                "This preview is no longer current. Review again."
            )
        return {
            "success": True,
            "preview": existing[1],
            "archive_path": existing[3],
        }
    archive_path = None
    archive_created = False
    try:
        with connect(_conninfo(db)) as conn, conn.cursor() as cursor:
            _ensure_schema(cursor)
            goal, table_name, mapping, status, preview, stored_preview_id = _session(
                cursor, import_id, lock=True
            )
            if status != "reviewed" or not preview:
                raise ImportValidationError("Review this import before confirming it.")
            if str(stored_preview_id) != str(preview_id):
                raise ImportValidationError(
                    "This preview is no longer current. Review again."
                )
            cursor.execute(
                sql.SQL(
                    "SELECT source_name, source_data FROM {}.import_sessions WHERE import_id = %s"
                ).format(sql.Identifier(SCHEMA)),
                (import_id,),
            )
            source_name, source_data = cursor.fetchone()
            archive_path, archive_created = _write_source_archive(
                table_name, source_name, source_data, import_id
            )
            stored_columns = list(mapping.values())
            empty_noop = preview.get("source_count") == 0 and goal in {
                "append",
                "merge",
            }
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
                target_columns = _target_columns(cursor, table_name)
                if (
                    _target_fingerprint(cursor, table_name)
                    != preview["target_fingerprint"]
                ):
                    raise ImportValidationError(
                        "The target dataset changed after review. Review the import again."
                    )
            if not empty_noop:
                for column in stored_columns:
                    if column not in target_columns:
                        cursor.execute(
                            sql.SQL("ALTER TABLE {} ADD COLUMN {} TEXT").format(
                                _quoted_table(table_name), sql.Identifier(column)
                            )
                        )
                        target_columns[column] = "text"
                _ensure_dataset_mapping(cursor, table_name, mapping)
            if goal == "create":
                cursor.execute(
                    sql.SQL(
                        "UPDATE {}.dataset_registry SET status = 'active', source_import_id = NULL WHERE target_table = %s AND source_import_id = %s"
                    ).format(sql.Identifier(SCHEMA)),
                    (table_name, import_id),
                )
            columns = [column for column in stored_columns if column in target_columns]
            insert_columns = ["_id", *columns]
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
                if cursor.rowcount != preview["added"]:
                    raise ImportValidationError(
                        "The applied row count did not match the reviewed import. Review again."
                    )
            elif not identity_columns:
                pass
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
                        changes = sql.SQL(" OR ").join(
                            sql.SQL(
                                "(s.payload ? {} AND (s.payload ->> {}) IS DISTINCT FROM t.{})"
                            ).format(
                                sql.Literal(column),
                                sql.Literal(column),
                                sql.Identifier(column),
                            )
                            for column in columns
                            if column != "_id"
                        )
                        cursor.execute(
                            sql.SQL(
                                "UPDATE {} t SET {} FROM {}.import_rows s WHERE s.import_id = %s AND {} AND ({})"
                            ).format(
                                _quoted_table(table_name),
                                assignments,
                                sql.Identifier(SCHEMA),
                                condition,
                                changes,
                            ),
                            (import_id,),
                        )
                        if cursor.rowcount != preview["updated"]:
                            raise ImportValidationError(
                                "The applied update count did not match the reviewed import. Review again."
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
                if cursor.rowcount != preview["added"]:
                    raise ImportValidationError(
                        "The applied row count did not match the reviewed import. Review again."
                    )
                if goal == "sync":
                    cursor.execute(
                        sql.SQL(
                            "DELETE FROM {} t WHERE NOT EXISTS (SELECT 1 FROM {}.import_rows s WHERE s.import_id = %s AND {})"
                        ).format(
                            _quoted_table(table_name),
                            sql.Identifier(SCHEMA),
                            condition,
                        ),
                        (import_id,),
                    )
                    if cursor.rowcount != preview["deleted"]:
                        raise ImportValidationError(
                            "The applied deletion count did not match the reviewed import. Review again."
                        )
            cursor.execute(
                sql.SQL(
                    "UPDATE {}.import_sessions SET status = 'archived', applied_at = %s, archive_path = %s, source_data = NULL WHERE import_id = %s"
                ).format(sql.Identifier(SCHEMA)),
                (datetime.now(UTC), archive_path, import_id),
            )
    except Exception:
        if archive_created and archive_path:
            with suppress(OSError):
                Path(archive_path).unlink(missing_ok=True)
        raise
    return {"success": True, "preview": preview, "archive_path": archive_path}
