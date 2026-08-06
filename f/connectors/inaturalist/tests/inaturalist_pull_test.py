import json

import psycopg
import pytest

from f.connectors.inaturalist.inaturalist_pull import (
    main,
    transform_observations_to_geojson,
)
from f.connectors.inaturalist.tests.assets.server_responses import (
    PRIMARY_OBSERVATION_ID,
    PRIMARY_PHOTO_FILENAME,
    PROJECT_ID,
    USERNAME,
)


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
        assert json.load(f)["slug"] == "lake-accotink-park"

    raw_path = asset_storage / table_name / f"{table_name}_observations.json"
    assert raw_path.exists()
    with open(raw_path) as f:
        assert len(json.load(f)) == 10

    geojson_path = asset_storage / table_name / f"{table_name}.geojson"
    assert geojson_path.exists()
    with open(geojson_path) as f:
        geojson_data = json.load(f)
        assert geojson_data["type"] == "FeatureCollection"
        assert len(geojson_data["features"]) == 10

    attachments = asset_storage / table_name / "attachments"
    assert (attachments / PRIMARY_PHOTO_FILENAME).exists()

    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            assert cur.fetchone()[0] == 10

            cur.execute(
                f"SELECT g__type, g__coordinates, data_source, scientific_name, "
                f"photo_url, photo_filename, project_id FROM {table_name} "
                f"WHERE _id = %s",
                (str(PRIMARY_OBSERVATION_ID),),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == "Point"
            assert row[1] == "[-77.227944, 38.803349]"
            assert row[2] == "iNaturalist"
            assert row[3] == "Lithobates sylvaticus"
            assert row[4] == (
                "https://inaturalist-open-data.s3.amazonaws.com/photos/9408078/medium.jpg"
            )
            assert row[5] == PRIMARY_PHOTO_FILENAME
            assert row[6] == PROJECT_ID


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
            assert cur.fetchone()[0] == 10

            cur.execute(
                f"SELECT data_source, user_id, scientific_name FROM {table_name} "
                f"WHERE _id = %s",
                (str(PRIMARY_OBSERVATION_ID),),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == "iNaturalist"
            assert row[1] == USERNAME
            assert row[2] == "Lithobates sylvaticus"


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
            assert cur.fetchone()[0] == 10

            cur.execute(f"SELECT _id FROM {table_name} ORDER BY _id")
            ids = {row[0] for row in cur.fetchall()}
            assert str(PRIMARY_OBSERVATION_ID) in ids
            assert "7288932" in ids


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
