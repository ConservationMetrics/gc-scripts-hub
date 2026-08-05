import json

import psycopg

from f.connectors.inaturalist.inaturalist_pull_my_observations import main
from f.connectors.inaturalist.inaturalist_pull_project import (
    transform_observations_to_geojson,
)
from f.connectors.inaturalist.tests.assets.server_responses import (
    PRIMARY_OBSERVATION_ID,
    PRIMARY_PHOTO_FILENAME,
    USERNAME,
)


def test_script_e2e(inaturalist_user_server, pg_database, tmp_path):
    asset_storage = tmp_path / "datalake"
    table_name = "inat_user_obs"

    main(
        inaturalist_user_server.username,
        pg_database,
        table_name,
        attachment_root=asset_storage,
    )

    # User pull does not fetch project metadata
    assert not (asset_storage / table_name / f"{table_name}_project.json").exists()

    raw_path = asset_storage / table_name / f"{table_name}_observations.json"
    assert raw_path.exists()
    with open(raw_path) as f:
        assert len(json.load(f)) == 10

    geojson_path = asset_storage / table_name / f"{table_name}.geojson"
    assert geojson_path.exists()
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


def test_script_e2e__no_observations(
    inaturalist_user_server_empty, pg_database, tmp_path
):
    asset_storage = tmp_path / "datalake"
    table_name = "inat_user_empty"

    main(
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


def test_transform_sets_user_id():
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
