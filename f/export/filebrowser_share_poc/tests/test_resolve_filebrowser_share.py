import json
from unittest.mock import MagicMock, patch

import pytest
import responses

from f.export.filebrowser_share_poc.resolve_filebrowser_share import main, report_views

ORIGIN = "https://files.example.com"
URL = f"{ORIGIN}/api/public/dl/hash"


def view(view_id, config):
    return {
        "view_id": view_id,
        "view_name": f"view {view_id}",
        "view_type": "map",
        "primary_dataset": "dataset",
        "view_config": json.dumps(config),
    }


def filebrowser(scope="/project"):
    responses.post(f"{ORIGIN}/api/login", body="jwt")
    responses.get(
        f"{ORIGIN}/api/shares",
        json=[{"hash": "hash", "userID": 3, "path": "/media", "expire": 0}],
    )
    responses.get(f"{ORIGIN}/api/users/3", json={"scope": scope})


@responses.activate
def test_all_views_fields_cached_and_mount(tmp_path):
    directory = tmp_path / "datalake/project/media"
    directory.mkdir(parents=True)
    (directory / "photo.jpg").write_text("photo")
    filebrowser()
    report = report_views(
        [
            view(
                1,
                {
                    "MEDIA_BASE_PATH": URL,
                    "MEDIA_BASE_PATH_ICONS": URL,
                    "MEDIA_BASE_PATH_ALERTS": "https://another.example/share/x",
                },
            ),
            view(2, {"MEDIA_BASE_PATH": f"{ORIGIN}/share/missing"}),
            view(3, {}),
            {**view(4, {}), "view_config": "{"},
        ],
        ORIGIN,
        "admin",
        "secret",
        "datalake",
        str(tmp_path),
    )
    assert report["views_scanned"] == 4
    assert [row["status"] for row in report["results"]] == [
        "resolved_candidate",
        "other_origin",
        "resolved_candidate",
        "share_not_found_or_ambiguous",
        "no_media_configured",
        "invalid_view_config",
    ]
    first = report["results"][0]
    assert first["candidate_smb_relative_path"] == "datalake/project/media"
    assert first["mount_check"]["entries"] == ["photo.jpg"]
    assert first["verified"] is False
    assert report["summary"]["other_origins"] == {"https://another.example": 1}
    assert report["summary"]["shares"] == 2
    assert (
        report["shares"][0]["candidate_smb_relative_path"] == "datalake/project/media"
    )
    assert [ref["field"] for ref in report["shares"][0]["references"]] == [
        "MEDIA_BASE_PATH",
        "MEDIA_BASE_PATH_ICONS",
    ]
    assert report["results"][3]["match_count"] == 0
    assert len(responses.calls) == 3
    assert responses.calls[1].request.headers["X-Auth"] == "jwt"
    assert "secret" not in json.dumps(report)


@responses.activate
def test_untrusted_host_never_receives_credentials():
    report = report_views(
        [view(1, {"MEDIA_BASE_PATH": "https://attacker.example/share/hash"})],
        ORIGIN,
        "admin",
        "secret",
        "datalake",
        "",
    )
    assert report["results"][0]["status"] == "other_origin"
    assert not responses.calls


@responses.activate
def test_ambiguous_share_and_origin_redaction():
    responses.post(f"{ORIGIN}/api/login", body="jwt")
    responses.get(f"{ORIGIN}/api/shares", json=[{"hash": "hash"}, {"hash": "hash"}])
    report = report_views(
        [
            view(
                1,
                {
                    "MEDIA_BASE_PATH": URL,
                    "MEDIA_BASE_PATH_ICONS": "https://user:password@other.example:8443/share/private?token=secret",
                },
            )
        ],
        ORIGIN,
        "admin",
        "secret",
        "datalake",
        "",
    )
    assert report["results"][0]["match_count"] == 2
    assert report["results"][1]["origin"] == "https://other.example:8443"
    assert "password" not in json.dumps(report)
    assert "private" not in json.dumps(report)


@responses.activate
def test_invalid_scope_and_mount():
    filebrowser("/../escape")
    report = report_views(
        [view(1, {"MEDIA_BASE_PATH": URL})], ORIGIN, "admin", "secret", "datalake", ""
    )
    assert report["results"][0]["status"] == "resolution_error"
    responses.reset()
    filebrowser(".")
    report = report_views(
        [view(1, {"MEDIA_BASE_PATH": URL})],
        ORIGIN,
        "admin",
        "secret",
        "datalake",
        "/missing-mount",
    )
    assert report["results"][0]["candidate_smb_relative_path"] == "datalake/media"
    assert (
        report["results"][0]["mount_check"]["status"] == "unavailable_or_outside_mount"
    )


@responses.activate
def test_login_failure_is_reported_for_all_fields():
    responses.post(f"{ORIGIN}/api/login", status=403)
    report = report_views(
        [view(1, {"MEDIA_BASE_PATH": URL, "MEDIA_BASE_PATH_ICONS": URL})],
        ORIGIN,
        "admin",
        "secret",
        "datalake",
        "",
    )
    assert [row["status"] for row in report["results"]] == [
        "filebrowser_unavailable",
        "filebrowser_unavailable",
    ]
    assert report["results"][0]["reason"] == "Filebrowser API returned HTTP 403"


def test_main_reads_only_expected_database():
    db = {
        "host": "db",
        "port": 5432,
        "user": "user",
        "password": "some password",
        "dbname": "guardianconnector",
    }
    connection = MagicMock()
    cursor = (
        connection.__enter__.return_value.cursor.return_value.__enter__.return_value
    )
    cursor.fetchall.return_value = [view(1, {})]
    with patch(
        "f.export.filebrowser_share_poc.resolve_filebrowser_share.connect",
        return_value=connection,
    ) as connect_mock:
        report = main(db, ORIGIN, "admin", "secret")
        assert report["views_scanned"] == 1
        assert cursor.execute.call_args_list[0].args[0] == "SET TRANSACTION READ ONLY"
        assert "FROM public.views" in cursor.execute.call_args_list[1].args[0]
        assert connect_mock.call_args.kwargs["password"] == "some password"
        with pytest.raises(ValueError, match="must match"):
            main({**db, "dbname": "warehouse"}, ORIGIN, "admin", "secret")
        assert connect_mock.call_count == 1
