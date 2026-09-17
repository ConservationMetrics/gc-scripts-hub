import json
from datetime import datetime
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import psycopg
import pytest

from f.connectors.inaturalist.inaturalist_pull import (
    _OBSERVATION_FIELDS,
    _encode_fields,
    _field_spec_paths,
    _iter_media,
    download_observations,
    main,
    transform_observations_to_geojson,
)
from f.connectors.inaturalist.tests.assets.server_responses import (
    CAPTIVE_OBSERVATION_ID,
    EMPTY_DESCRIPTION_OBSERVATION_ID,
    MULTI_PHOTO_OBSERVATION_ID,
    NEEDS_ID_OBSERVATION_ID,
    NULL_ACCURACY_OBSERVATION_ID,
    NULL_TAXON_OBSERVATION_ID,
    OBSCURED_TAXON_GEOPRIVACY_ID,
    OBSERVATION_COUNT,
    PRIMARY_GBIF_OCCURRENCE_ID,
    PRIMARY_OBSERVATION_ID,
    PRIMARY_PHOTO_FILENAME,
    PROJECT_ID,
    SOUND_FILENAME,
    SOUND_ONLY_OBSERVATION_ID,
    SYNTHETIC_OBSERVER_ORCID,
    USERNAME,
    _load_observations,
)

LAKE_ACCOTINK_BBOX = "[[-77.22182, 38.79260], [-77.21899, 38.79402]]"
LAKE_ACCOTINK_QUERY = {
    "swlng": -77.22182,
    "swlat": 38.79260,
    "nelng": -77.21899,
    "nelat": 38.79402,
}

_TRANSFORM_READS = {
    "id",
    "uuid",
    "uri",
    "updated_at",
    "observed_on",
    "time_observed_at",
    "observed_time_zone",
    "quality_grade",
    "captive",
    "mappable",
    "geojson",
    "place_guess",
    "positional_accuracy",
    "public_positional_accuracy",
    "obscured",
    "geoprivacy",
    "taxon_geoprivacy",
    "species_guess",
    "description",
    "license_code",
    "identifications_count",
    "community_taxon_id",
    "num_identification_agreements",
    "num_identification_disagreements",
    "outlinks",
    "outlinks.source",
    "outlinks.url",
    "taxon",
    "taxon.id",
    "taxon.name",
    "taxon.rank",
    "taxon.rank_level",
    "taxon.preferred_common_name",
    "taxon.iconic_taxon_name",
    "user",
    "user.id",
    "user.login",
    "user.name",
    "user.orcid",
    "photos",
    "photos.id",
    "photos.url",
    "photos.license_code",
    "photos.attribution",
    "sounds",
    "sounds.id",
    "sounds.file_url",
}


def _observation_queries(mocked_responses) -> list[dict[str, list[str]]]:
    return [
        parse_qs(urlparse(call.request.url).query)
        for call in mocked_responses.calls
        if "/v2/observations" in call.request.url
    ]


def _row(cur, table_name: str, observation_id: int, columns: list[str]) -> dict:
    cur.execute(
        f"SELECT {', '.join(columns)} FROM {table_name} WHERE _id = %s",
        (str(observation_id),),
    )
    values = cur.fetchone()
    assert values is not None
    return dict(zip(columns, values))


def _truthy(value) -> bool:
    return str(value).lower() in {"true", "t", "1"}


def _first_observation_params(mocked_responses) -> dict[str, str]:
    for call in mocked_responses.calls:
        parsed = urlparse(call.request.url)
        if parsed.path.rstrip("/").endswith("/observations"):
            return {key: values[0] for key, values in parse_qs(parsed.query).items()}
    raise AssertionError("expected an iNaturalist observations request")


def _assert_bbox_params(params: dict[str, str], bbox: dict[str, float]) -> None:
    for key, value in bbox.items():
        assert float(params[key]) == value


def test_transform_keys_are_in_field_spec():
    assert _TRANSFORM_READS <= _field_spec_paths(_OBSERVATION_FIELDS)


def test_project_e2e(inaturalist_project_server, pg_database, tmp_path):
    asset_storage = tmp_path / "datalake"
    table_name = "inat_observations"

    main(
        "project",
        inaturalist_project_server.project_id,
        pg_database,
        table_name,
        attachment_root=asset_storage,
    )

    project_path = asset_storage / table_name / f"{table_name}_project.json"
    assert project_path.exists()
    with open(project_path) as f:
        project = json.load(f)
        assert project["slug"] == "lake-accotink-park"
        assert project["rule_preferences"][0]["value"] == "research,needs_id"

    raw_path = asset_storage / table_name / f"{table_name}_observations.json"
    assert raw_path.exists()
    with open(raw_path) as f:
        assert len(json.load(f)) == OBSERVATION_COUNT

    geojson_path = asset_storage / table_name / f"{table_name}.geojson"
    assert geojson_path.exists()
    with open(geojson_path) as f:
        geojson_data = json.load(f)
        assert geojson_data["type"] == "FeatureCollection"
        assert len(geojson_data["features"]) == OBSERVATION_COUNT

    attachments = asset_storage / table_name / "attachments"
    assert (attachments / PRIMARY_PHOTO_FILENAME).exists()
    assert (attachments / SOUND_FILENAME).exists()

    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            assert cur.fetchone()[0] == OBSERVATION_COUNT

            primary = _row(
                cur,
                table_name,
                PRIMARY_OBSERVATION_ID,
                [
                    "g__type",
                    "g__coordinates",
                    "data_source",
                    "scientific_name",
                    "photo_url",
                    "photo_filename",
                    "project_id",
                    "gbif_occurrence_id",
                    "uuid",
                    "observer",
                ],
            )
            assert primary["g__type"] == "Point"
            assert primary["g__coordinates"] == "[-77.227944, 38.803349]"
            assert primary["data_source"] == "iNaturalist"
            assert primary["scientific_name"] == "Lithobates sylvaticus"
            assert primary["photo_url"] == (
                "https://inaturalist-open-data.s3.amazonaws.com/photos/9408078/medium.jpg"
            )
            assert primary["photo_filename"] == PRIMARY_PHOTO_FILENAME
            assert primary["project_id"] == PROJECT_ID
            assert primary["gbif_occurrence_id"] == PRIMARY_GBIF_OCCURRENCE_ID
            assert primary["uuid"]
            assert primary["observer"] == USERNAME

            obscured = _row(
                cur,
                table_name,
                OBSCURED_TAXON_GEOPRIVACY_ID,
                [
                    "obscured",
                    "taxon_geoprivacy",
                    "positional_accuracy",
                    "public_positional_accuracy",
                ],
            )
            assert _truthy(obscured["obscured"])
            assert obscured["taxon_geoprivacy"] == "obscured"
            assert str(obscured["positional_accuracy"]) == "3"
            assert str(obscured["public_positional_accuracy"]) == "28210"

            captive = _row(cur, table_name, CAPTIVE_OBSERVATION_ID, ["captive"])
            assert _truthy(captive["captive"])

            null_taxon = _row(
                cur, table_name, NULL_TAXON_OBSERVATION_ID, ["taxon_rank", "taxon_id"]
            )
            assert null_taxon["taxon_rank"] is None
            assert null_taxon["taxon_id"] is None

            sound_only = _row(
                cur,
                table_name,
                SOUND_ONLY_OBSERVATION_ID,
                ["sound_count", "photo_count", "sound_filenames", "photo_filename"],
            )
            assert str(sound_only["sound_count"]) == "1"
            assert str(sound_only["photo_count"]) == "0"
            assert sound_only["sound_filenames"] == SOUND_FILENAME
            assert sound_only["photo_filename"] is None

            needs_id = _row(
                cur,
                table_name,
                NEEDS_ID_OBSERVATION_ID,
                [
                    "quality_grade",
                    "gbif_occurrence_id",
                    "community_taxon_id",
                    "taxon_id",
                    "observer_name",
                    "observer_orcid",
                ],
            )
            assert needs_id["quality_grade"] == "needs_id"
            assert needs_id["gbif_occurrence_id"] is None
            assert str(needs_id["community_taxon_id"]) == "52861"
            assert str(needs_id["taxon_id"]) == "1473617"
            assert needs_id["observer_name"] == "Kathleen Murray"
            assert needs_id["observer_orcid"] == SYNTHETIC_OBSERVER_ORCID


def test_user_e2e(inaturalist_user_server, pg_database, tmp_path):
    asset_storage = tmp_path / "datalake"
    table_name = "inat_user_obs"

    main(
        "user",
        inaturalist_user_server.username,
        pg_database,
        table_name,
        attachment_root=asset_storage,
    )

    assert not (asset_storage / table_name / f"{table_name}_project.json").exists()
    assert (
        asset_storage / table_name / "attachments" / PRIMARY_PHOTO_FILENAME
    ).exists()

    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            assert cur.fetchone()[0] == OBSERVATION_COUNT

            row = _row(
                cur,
                table_name,
                PRIMARY_OBSERVATION_ID,
                ["data_source", "user_id", "scientific_name"],
            )
            assert row["data_source"] == "iNaturalist"
            assert row["user_id"] == USERNAME
            assert row["scientific_name"] == "Lithobates sylvaticus"


def test_pagination(inaturalist_project_server_paginated, pg_database, tmp_path):
    asset_storage = tmp_path / "datalake"
    table_name = "inat_paginated"

    main(
        "project",
        inaturalist_project_server_paginated.project_id,
        pg_database,
        table_name,
        attachment_root=asset_storage,
    )

    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            assert cur.fetchone()[0] == OBSERVATION_COUNT

            cur.execute(f"SELECT _id FROM {table_name} ORDER BY _id")
            ids = {row[0] for row in cur.fetchall()}
            assert str(PRIMARY_OBSERVATION_ID) in ids
            assert str(NULL_ACCURACY_OBSERVATION_ID) in ids


def test_project_e2e__no_observations(
    inaturalist_project_server_empty, pg_database, tmp_path
):
    asset_storage = tmp_path / "datalake"
    table_name = "inat_no_obs"

    main(
        "project",
        inaturalist_project_server_empty.project_id,
        pg_database,
        table_name,
        attachment_root=asset_storage,
    )

    assert not (asset_storage / table_name / f"{table_name}.geojson").exists()

    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s)", (f"public.{table_name}",))
            assert cur.fetchone()[0] is None


def test_user_e2e__no_observations(
    inaturalist_user_server_empty, pg_database, tmp_path
):
    asset_storage = tmp_path / "datalake"
    table_name = "inat_user_empty"

    main(
        "user",
        inaturalist_user_server_empty.username,
        pg_database,
        table_name,
        attachment_root=asset_storage,
    )

    assert not (asset_storage / table_name / f"{table_name}.geojson").exists()

    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s)", (f"public.{table_name}",))
            assert cur.fetchone()[0] is None


def test_invalid_source(pg_database, tmp_path):
    with pytest.raises(ValueError, match="Invalid source"):
        main(
            "not-a-source",
            "anything",
            pg_database,
            "inat_bad",
            attachment_root=tmp_path / "datalake",
        )


def test_download_observations(mocked_responses, inaturalist_observations_server):
    observations = download_observations({"project_id": PROJECT_ID})
    assert len(observations) == OBSERVATION_COUNT
    assert observations[0]["id"] == PRIMARY_OBSERVATION_ID

    queries = _observation_queries(mocked_responses)
    assert len(queries) == 1
    assert queries[0]["project_id"] == [PROJECT_ID]
    assert queries[0]["order_by"] == ["id"]
    assert queries[0]["order"] == ["asc"]
    assert queries[0]["fields"] == [_encode_fields(_OBSERVATION_FIELDS)]
    assert "id_above" not in queries[0]


def test_download_observations_pagination(
    mocked_responses, inaturalist_observations_server_paginated
):
    observations = download_observations({"project_id": PROJECT_ID})
    ids = [observation["id"] for observation in observations]
    assert ids == sorted(ids)
    assert len(ids) == OBSERVATION_COUNT
    assert len(set(ids)) == OBSERVATION_COUNT

    queries = _observation_queries(mocked_responses)
    assert "id_above" not in queries[0]
    assert [int(q["id_above"][0]) for q in queries[1:]] == ids[1::2]


def test_download_observations_empty(inaturalist_observations_server_empty):
    assert download_observations({"project_id": PROJECT_ID}) == []


def test_download_observations_stuck_cursor(inaturalist_stuck_cursor_server):
    with pytest.raises(RuntimeError, match="Pagination cursor did not advance"):
        download_observations({"project_id": PROJECT_ID})


def test_iter_media():
    observations = [
        {
            "photos": [
                {"id": 1, "url": "https://example.com/photos/1/square.jpg"},
                {"id": 2, "url": None},
                {"url": "https://example.com/photos/3/square.jpg"},
            ],
            "sounds": [
                {"id": 10, "file_url": "https://static.inaturalist.org/sounds/10.m4a"},
                {"id": 11},
            ],
        }
    ]
    assert list(_iter_media(observations)) == [
        ("https://example.com/photos/1/original.jpg", "1.jpg"),
        ("https://static.inaturalist.org/sounds/10.m4a", "10.m4a"),
    ]


def test_iter_media_fixture():
    media = {
        filename: url for url, filename in _iter_media(_load_observations()["results"])
    }
    assert media[PRIMARY_PHOTO_FILENAME] == (
        "https://inaturalist-open-data.s3.amazonaws.com/photos/9408078/original.jpg"
    )
    assert SOUND_FILENAME in media
    assert "/square." not in media[PRIMARY_PHOTO_FILENAME]


def test_slug_only_omits_bbox_params(
    inaturalist_project_server, mocked_responses, pg_database, tmp_path
):
    main(
        "project",
        inaturalist_project_server.project_id,
        pg_database,
        "inat_slug_only",
        attachment_root=tmp_path / "datalake",
    )
    params = _first_observation_params(mocked_responses)
    assert params["project_id"] == PROJECT_ID
    assert "user_id" not in params
    assert not {"swlat", "swlng", "nelat", "nelng"} & params.keys()


def test_slug_with_empty_bbox_omits_bbox_params(
    inaturalist_project_server, mocked_responses, pg_database, tmp_path
):
    main(
        "project",
        inaturalist_project_server.project_id,
        pg_database,
        "inat_empty_bbox",
        attachment_root=tmp_path / "datalake",
        bounding_box="",
    )
    params = _first_observation_params(mocked_responses)
    assert params["project_id"] == PROJECT_ID
    assert not {"swlat", "swlng", "nelat", "nelng"} & params.keys()


def test_bbox_only_sends_bbox_params(
    inaturalist_user_server, mocked_responses, pg_database, tmp_path
):
    asset_storage = tmp_path / "datalake"
    table_name = "inat_bbox_only"
    main(
        None,
        None,
        pg_database,
        table_name,
        attachment_root=asset_storage,
        bounding_box=LAKE_ACCOTINK_BBOX,
    )
    params = _first_observation_params(mocked_responses)
    _assert_bbox_params(params, LAKE_ACCOTINK_QUERY)
    assert "project_id" not in params
    assert "user_id" not in params
    assert not (asset_storage / table_name / f"{table_name}_project.json").exists()

    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            assert cur.fetchone()[0] == OBSERVATION_COUNT


def test_slug_and_bbox_sends_both_filters(
    inaturalist_project_server, mocked_responses, pg_database, tmp_path
):
    main(
        "project",
        inaturalist_project_server.project_id,
        pg_database,
        "inat_slug_bbox",
        attachment_root=tmp_path / "datalake",
        bounding_box=LAKE_ACCOTINK_BBOX,
    )
    params = _first_observation_params(mocked_responses)
    assert params["project_id"] == PROJECT_ID
    _assert_bbox_params(params, LAKE_ACCOTINK_QUERY)


@patch("f.common_logic.date_utils.datetime")
def test_max_months_lookback_sends_d1(
    mock_datetime, inaturalist_project_server, mocked_responses, pg_database, tmp_path
):
    mock_datetime.now.return_value = datetime(2025, 10, 15)
    main(
        "project",
        inaturalist_project_server.project_id,
        pg_database,
        "inat_lookback",
        attachment_root=tmp_path / "datalake",
        max_months_lookback=6,
    )
    params = _first_observation_params(mocked_responses)
    assert params["d1"] == "2025-04-01"
    assert params["project_id"] == PROJECT_ID


def test_no_lookback_omits_d1(
    inaturalist_project_server, mocked_responses, pg_database, tmp_path
):
    main(
        "project",
        inaturalist_project_server.project_id,
        pg_database,
        "inat_no_lookback",
        attachment_root=tmp_path / "datalake",
    )
    params = _first_observation_params(mocked_responses)
    assert "d1" not in params


def test_bbox_json_string_sends_bbox_params(
    inaturalist_user_server, mocked_responses, pg_database, tmp_path
):
    main(
        None,
        None,
        pg_database,
        "inat_bbox_json",
        attachment_root=tmp_path / "datalake",
        bounding_box="""
        [
          [-77.22182, 38.79260],
          [-77.21899, 38.79402]
        ]
        """,
    )
    _assert_bbox_params(_first_observation_params(mocked_responses), LAKE_ACCOTINK_QUERY)


def test_neither_slug_nor_bbox_raises(pg_database, tmp_path):
    with pytest.raises(ValueError, match="Either `slug` or `bounding_box`"):
        main(
            None,
            None,
            pg_database,
            "inat_none",
            attachment_root=tmp_path / "datalake",
        )


def test_incomplete_bbox_raises(pg_database, tmp_path):
    with pytest.raises(ValueError, match="exactly two"):
        main(
            None,
            None,
            pg_database,
            "inat_incomplete_bbox",
            attachment_root=tmp_path / "datalake",
            bounding_box="[[-77.22182, 38.79260]]",
        )


@pytest.mark.parametrize(
    "bbox, match",
    [
        (
            "[[-77.22182, 91], [-77.21899, 38.79402]]",
            "nelat must be between -90 and 90",
        ),
        (
            "[[-181, 38.79260], [-77.21899, 38.79402]]",
            "swlng must be between -180 and 180",
        ),
        (
            "[[-77.22182, 38.79402], [-77.21899, 38.79402]]",
            "swlat must be less than nelat",
        ),
        (
            "[[-77.22182, 38.79260], [-77.22182, 38.79402]]",
            "swlng must be less than nelng",
        ),
    ],
)
def test_invalid_bbox_raises(pg_database, tmp_path, bbox, match):
    with pytest.raises(ValueError, match=match):
        main(
            None,
            None,
            pg_database,
            "inat_bad_bbox",
            attachment_root=tmp_path / "datalake",
            bounding_box=bbox,
        )


def test_transform_with_location():
    observations = [
        {
            "id": 101,
            "observed_on": "2026-07-10",
            "quality_grade": "research",
            "species_guess": "Wood Frog",
            "geojson": {"type": "Point", "coordinates": [-77.22, 38.80]},
            "taxon": {
                "id": 66012,
                "name": "Lithobates sylvaticus",
                "preferred_common_name": "Wood Frog",
            },
            "user": {"login": "observer1"},
            "uri": "https://www.inaturalist.org/observations/101",
            "license_code": "cc-by-nc",
            "photos": [
                {
                    "id": 1,
                    "url": "https://example.com/photos/1/square.jpg",
                }
            ],
        }
    ]
    result = transform_observations_to_geojson(observations, project_id=PROJECT_ID)

    feature = result["features"][0]
    props = feature["properties"]
    assert props["data_source"] == "iNaturalist"
    assert props["project_id"] == PROJECT_ID
    assert "user_id" not in props
    assert props["photo_url"] == "https://example.com/photos/1/medium.jpg"
    assert props["photo_filename"] == "1.jpg"


def test_transform_user_id():
    observations = [
        {
            "id": 303,
            "observed_on": "2026-01-01",
            "quality_grade": "casual",
            "species_guess": None,
            "geojson": {"type": "Point", "coordinates": [-77.0, 38.0]},
            "taxon": None,
            "user": {"login": USERNAME},
            "uri": "https://www.inaturalist.org/observations/303",
            "license_code": None,
            "photos": [],
        }
    ]
    result = transform_observations_to_geojson(observations, user_id=USERNAME)
    props = result["features"][0]["properties"]
    assert props["user_id"] == USERNAME
    assert "project_id" not in props


def test_transform_no_location():
    observations = [
        {
            "id": 202,
            "observed_on": None,
            "quality_grade": "needs_id",
            "species_guess": None,
            "geojson": None,
            "taxon": None,
            "user": {},
            "uri": "https://www.inaturalist.org/observations/202",
            "license_code": None,
            "photos": [],
        }
    ]
    result = transform_observations_to_geojson(observations, project_id=PROJECT_ID)

    feature = result["features"][0]
    assert feature["geometry"] is None
    assert feature["properties"]["photo_url"] is None
    assert feature["properties"]["photo_filename"] is None


def test_transform_photo_filenames():
    observation = next(
        o
        for o in _load_observations()["results"]
        if o["id"] == MULTI_PHOTO_OBSERVATION_ID
    )
    props = transform_observations_to_geojson([observation])["features"][0][
        "properties"
    ]
    filenames = props["photo_filenames"].split(", ")
    assert props["photo_count"] == 10
    assert len(filenames) == 10
    assert props["photo_filename"] == filenames[0]
    assert "/medium." in props["photo_url"]


def test_transform_description_empty_vs_null():
    by_id = {o["id"]: o for o in _load_observations()["results"]}
    empty = transform_observations_to_geojson(
        [by_id[EMPTY_DESCRIPTION_OBSERVATION_ID]]
    )["features"][0]["properties"]
    missing = transform_observations_to_geojson([by_id[NEEDS_ID_OBSERVATION_ID]])[
        "features"
    ][0]["properties"]
    assert empty["description"] == ""
    assert missing["description"] is None
