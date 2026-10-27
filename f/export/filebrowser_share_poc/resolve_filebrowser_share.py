"""Report candidate storage paths for Explorer's Filebrowser media shares."""

# requirements:
# psycopg[binary]==3.3.3
# requests==2.32.5

import json
import os
import re
import tempfile
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit

import requests
from psycopg import Error, connect
from psycopg.rows import dict_row

from f.common_logic.db_operations import postgresql

MEDIA_FIELDS = ("MEDIA_BASE_PATH", "MEDIA_BASE_PATH_ALERTS", "MEDIA_BASE_PATH_ICONS")
SHARE_PATH = re.compile(r"^/(?:share|api/public/dl)/([A-Za-z0-9_-]+)/?$")


def clean_path(value):
    """Validate a path component and return it relative to its root."""
    if not isinstance(value, str) or "\\" in value or "\0" in value:
        raise ValueError("Invalid storage path")
    parts = value.split("/")
    if any(part in (".", "..") for part in parts):
        raise ValueError("Invalid storage path")
    return "/".join(part for part in parts if part)


def report_views(
    views, filebrowser_origin, username, password, storage_prefix, mount_path
):
    """Resolve all media fields from a snapshot of Explorer views."""
    origin = urlsplit(filebrowser_origin)
    if (
        origin.scheme != "https"
        or not origin.hostname
        or origin.username
        or origin.password
        or origin.path not in ("", "/")
        or origin.query
        or origin.fragment
    ):
        raise ValueError("Filebrowser origin must be an HTTPS origin without a path")
    base = f"https://{origin.netloc}"
    prefix = clean_path(storage_prefix)
    if mount_path and not Path(mount_path).is_absolute():
        raise ValueError("mount_path must be absolute")

    results, pending = [], []
    for view in views:
        identity = {
            key: view[key]
            for key in ("view_id", "view_name", "view_type", "primary_dataset")
        }
        try:
            config = json.loads(view["view_config"])
            if not isinstance(config, dict):
                raise TypeError("Invalid view config")
        except (TypeError, ValueError):
            results.append({**identity, "status": "invalid_view_config"})
            continue
        populated = False
        for field in MEDIA_FIELDS:
            value = config.get(field)
            if value is None or value == "":
                continue
            populated = True
            row = {**identity, "field": field}
            results.append(row)
            if not isinstance(value, str):
                row["status"] = "invalid_media_value"
                continue
            try:
                url = urlsplit(value)
                host = url.hostname
                port = url.port
            except ValueError:
                row["status"] = "unsupported_media_value"
                continue
            if not host or not url.scheme:
                row["status"] = "unsupported_media_value"
                continue
            if (url.scheme.lower(), host.lower(), port or 443) != (
                "https",
                origin.hostname.lower(),
                origin.port or 443,
            ):
                row["status"] = "other_origin"
                continue
            match = SHARE_PATH.fullmatch(url.path)
            if not match or url.username or url.password or url.query or url.fragment:
                row["status"] = "unsupported_media_value"
                continue
            pending.append((row, match.group(1)))
        if not populated:
            results.append({**identity, "status": "no_media_configured"})

    report = {"dry_run": True, "views_scanned": len(views), "results": results}
    if not pending:
        return report

    with requests.Session() as session:

        def api(method, api_path, token=None, body=None):
            try:
                response = session.request(
                    method,
                    base + api_path,
                    headers={"X-Auth": token} if token else {},
                    json=body,
                    timeout=15,
                    allow_redirects=False,
                )
            except requests.RequestException as exc:
                raise ValueError("Filebrowser network error") from exc
            if not 200 <= response.status_code < 300:
                raise ValueError(
                    f"Filebrowser API returned HTTP {response.status_code}"
                )
            return response

        try:
            token = api(
                "POST", "/api/login", body={"username": username, "password": password}
            ).text.strip()
            if not token:
                raise ValueError("Filebrowser returned an empty login token")
            shares = api("GET", "/api/shares", token).json()
            if not isinstance(shares, list):
                raise TypeError("Filebrowser returned an invalid share list")
        except (TypeError, ValueError, requests.RequestException) as exc:
            reason = (
                str(exc)
                if str(exc).startswith("Filebrowser ")
                else "Filebrowser returned invalid data"
            )
            for row, _ in pending:
                row.update(status="filebrowser_unavailable", reason=reason)
            return report

        owners, cache = {}, {}
        for row, share_hash in pending:
            if share_hash not in cache:
                try:
                    matches = [
                        share
                        for share in shares
                        if isinstance(share, dict) and share.get("hash") == share_hash
                    ]
                    if len(matches) != 1:
                        candidate = {"status": "share_not_found_or_ambiguous"}
                    else:
                        share = matches[0]
                        owner_id = share.get("userID")
                        if (
                            not isinstance(owner_id, int)
                            or isinstance(owner_id, bool)
                            or owner_id < 1
                        ):
                            raise ValueError("Invalid share owner ID")
                        if owner_id not in owners:
                            owner = api("GET", f"/api/users/{owner_id}", token).json()
                            scope = (
                                owner.get("scope") if isinstance(owner, dict) else None
                            )
                            if not isinstance(scope, str) or not scope:
                                raise ValueError(
                                    "Owner scope unavailable; use an administrator account"
                                )
                            owners[owner_id] = "" if scope == "." else clean_path(scope)
                        scope = owners[owner_id]
                        share_path = clean_path(share.get("path"))
                        smb_path = str(PurePosixPath(prefix, scope, share_path))
                        candidate = {
                            "status": "resolved_candidate",
                            "owner_id": owner_id,
                            "owner_scope": f"/{scope}",
                            "share_path": f"/{share_path}",
                            "candidate_smb_relative_path": smb_path,
                            "share_expire": share.get("expire"),
                            "verified": False,
                        }
                        if mount_path:
                            try:
                                root = Path(mount_path).resolve(strict=True)
                                directory = (root / smb_path).resolve(strict=True)
                                if not directory.is_relative_to(root):
                                    raise ValueError("Path escapes mount")
                                entries = []
                                with os.scandir(directory) as items:
                                    for item in items:
                                        entries.append(item.name)
                                        if len(entries) == 21:
                                            break
                                candidate["mount_check"] = {
                                    "status": "directory_exists",
                                    "directory": str(directory),
                                    "entries": entries[:20],
                                    "truncated": len(entries) > 20,
                                }
                            except (OSError, ValueError):
                                candidate["mount_check"] = {
                                    "status": "unavailable_or_outside_mount"
                                }
                except (ValueError, requests.RequestException):
                    candidate = {
                        "status": "resolution_error",
                        "reason": "Unable to resolve share or owner scope",
                    }
                cache[share_hash] = candidate
            row.update(cache[share_hash])
    return report


def main(
    db: postgresql,
    filebrowser_origin: str,
    username: str,
    password: str,
    expected_config_database: str = "guardianconnector",
    storage_prefix: str = "datalake",
    mount_path: str = "",
):
    """Read Explorer views and return a dry-run migration report.

    Parameters
    ----------
    db : postgresql
        Windmill PostgreSQL resource for the Explorer configuration database.
    filebrowser_origin : str
        Trusted HTTPS origin of the Filebrowser instance.
    username : str
        Filebrowser administrator username.
    password : str
        Filebrowser password, supplied as a Windmill secret variable.
    expected_config_database : str
        Expected database name, to prevent accidentally selecting the warehouse.
    storage_prefix : str
        Filebrowser root relative to the SMB share.
    mount_path : str
        Optional worker mount path for checking candidate directories.

    Returns
    -------
    dict
        Read-only report with a status for each view and configured media field.
    """
    if not expected_config_database or db.get("dbname") != expected_config_database:
        raise ValueError(
            "PostgreSQL resource database must match expected_config_database"
        )
    mode = db.get("sslmode") or "disable"
    if mode not in ("disable", "require", "verify-ca", "verify-full"):
        raise ValueError("Unsupported Postgres sslmode")
    # Pass connection fields separately so special characters in passwords are preserved.
    options = {
        "host": db["host"],
        "port": db["port"],
        "user": db["user"],
        "password": db.get("password", ""),
        "dbname": db["dbname"],
        "sslmode": mode,
        "connect_timeout": 15,
        "row_factory": dict_row,
    }
    with tempfile.TemporaryDirectory() as tmpdir:
        if db.get("root_certificate_pem"):
            certificate = Path(tmpdir) / "root.crt"
            certificate.write_text(db["root_certificate_pem"])
            options["sslrootcert"] = str(certificate)
        try:
            with connect(**options) as connection, connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION READ ONLY")
                cursor.execute(
                    "SELECT view_id, view_name, view_type, primary_dataset, view_config FROM public.views ORDER BY view_id"
                )
                views = cursor.fetchall()
        except Error:
            raise RuntimeError(
                "Unable to read Explorer views; verify database access and schema"
            ) from None
    return report_views(
        views, filebrowser_origin, username, password, storage_prefix, mount_path
    )
