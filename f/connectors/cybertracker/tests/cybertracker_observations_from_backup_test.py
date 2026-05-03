import pytest

from f.connectors.cybertracker.cybertracker_observations_from_backup import (
    _looks_like_uid,
    _normalize_field_value,
    parse_cybertracker_json,
)


@pytest.mark.parametrize(
    "value,expected",
    [
        ("bcd9f6c79f414466bef9cae764297de5", True),
        (" 90056d9b51644094bb0ebceb1554bcc2 ", True),
        ("90056d9b-5164-4094-bb0e-bceb1554bcc2", True),
        ("not-a-uid", False),
        ("", False),
        (31 * "a" + "g", False),
        (None, False),
    ],
)
def test_looks_like_uid(value, expected):
    assert _looks_like_uid(value) is expected


def test_normalize_field_value_skips_list_of_dashed_uuids_like_hex32():
    """Repeat-parent uid lists normalize to ``None`` when every element is an id shape,
    including dashed RFC-4122 spelling (not present in the checked-in fixture)."""
    key = "repeat_s20260430153715926_photo_of_site"
    child_ids = [
        "90056d9b-5164-4094-bb0e-bceb1554bcc2",
        "bcd9f6c7-9f41-4466-bef9-cae764297de5",
    ]
    assert _normalize_field_value(key, child_ids) is None


def test_parse_cybertracker_json__feature_collection(cybertracker_json_path):
    geojson = parse_cybertracker_json(cybertracker_json_path)

    assert geojson["type"] == "FeatureCollection"
    assert "features" in geojson
    # Fixture has 4 sessions; first is KMZ track-only (skipped — no point x/y).
    assert len(geojson["features"]) == 3
    assert all(f["geometry"] is not None for f in geojson["features"])
    assert all(f["geometry"]["type"] == "Point" for f in geojson["features"])
    track_only_uid = "3cb38199e6174d17a11e864b438cd9e8"
    assert track_only_uid not in {f["id"] for f in geojson["features"]}


def test_parse_cybertracker_json__location_ping_has_geometry(cybertracker_json_path):
    """Plain ``location`` (not ``cto_location``) still yields a Point — regression for
    sessions that are observations but use the non-cto field name."""
    geojson = parse_cybertracker_json(cybertracker_json_path)
    feat = next(
        f for f in geojson["features"] if f["id"] == "cb5e04cf13124dca9f1f1665ef059642"
    )
    assert "location" in feat["properties"]
    assert feat["geometry"]["coordinates"] == [-77.0, 38.0]
    assert "dropped pin" in feat["properties"]["field_note"].lower()


def test_parse_cybertracker_json__field_mapping_and_geometry(cybertracker_json_path):
    geojson = parse_cybertracker_json(cybertracker_json_path)
    features = geojson["features"]

    # Forest expedition row (schema with photo_of_site + audio_recording).
    feat = next(
        f
        for f in features
        if "hornbill" in f["properties"].get("additional_note", "").lower()
    )

    props = feat["properties"]

    assert props["_id"] == "8bc8d8c38801469bb09a0e82d4724eb2"
    assert feat["id"] == props["_id"]

    assert props["_deviceid"] == "ct:6e94db170223de920000000000000000"
    assert props["_start"] == "2026-04-30T11:41:02.531-04:00"
    assert props["_end"] == "2026-04-30T11:41:02.531-04:00"
    assert props["_username"] == "field_team_alpha"

    assert props["number_of_species"] == 7
    assert props["audio_recording"] == "e234a9b27a4a494a9800a918b1d034ce.wav"
    assert props["photo_of_site"] == "PHOTO_20260430_114040.jpg"
    assert props["collect_data"] == [
        "s20260430153623050_collect_data/i20260430153647038_ceiba_canopy",
        "s20260430153623050_collect_data/i20260430153657095_fire_scar_history",
    ]

    assert props["_location"]["x"] == -77
    assert props["_location"]["y"] == 38

    assert feat["geometry"]["type"] == "Point"
    assert feat["geometry"]["coordinates"] == [-77.0, 38.0]


def test_parse_cybertracker_json__repeat_parent_uid_list_skipped(
    cybertracker_json_path,
):
    geojson = parse_cybertracker_json(cybertracker_json_path)
    features = geojson["features"]

    # Community mapping row: repeat parent holds child uids; filename comes from child.
    feat = next(
        f
        for f in features
        if "headwater spring" in f["properties"].get("type_of_sighting", "").lower()
    )
    assert feat["properties"]["photo_of_site"] == "PHOTO_20260430_144233.jpg"
