import json

import psycopg
import pytest

from f.connectors.inaturalist.inaturalist_pull import (
    _OBSERVATION_FIELDS,
    _field_spec_paths,
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
