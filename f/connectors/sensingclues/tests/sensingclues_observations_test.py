import json

import psycopg
import pytest
from sensingcluespy.src.exceptions import SCPermissionDenied

from f.connectors.sensingclues.sensingclues_observations import (
    main,
    transform_observations_to_geojson,
)
from f.connectors.sensingclues.tests.assets.server_responses import (
    AFRICA_ACTION_TAKEN,
    AFRICA_ENTITY_IDS,
    AFRICA_FILENAME,
    AFRICA_GROUP,
    AFRICA_PRIMARY_ENTITY_ID,
    CLUEY_GROUP,
    PRIMARY_AGENT_NAME,
    PRIMARY_BENEFICIARIES_TOTAL,
    PRIMARY_CONCEPT_LABELS,
    PRIMARY_COORDINATES,
    PRIMARY_ENTITY_ID,
    PRIMARY_PROJECT_NAME,
    observations_page,
)


def _run(server, db, table_name, attachment_root, group_identifier=None):
    main(
        server.username,
        server.password,
        group_identifier if group_identifier is not None else server.group_identifier,
        db,
        table_name,
        attachment_root=attachment_root,
    )


def test_e2e(sensingclues_server, pg_database, tmp_path):
    asset_storage = tmp_path / "datalake"
    table_name = "sc_observations"

    _run(sensingclues_server, pg_database, table_name, asset_storage)

    raw_path = asset_storage / table_name / f"{table_name}_observations.json"
    assert raw_path.exists()
    with open(raw_path) as f:
        raw = json.load(f)
    assert len(raw) == 2
    assert raw[0]["id"] == PRIMARY_ENTITY_ID

    geojson_path = asset_storage / table_name / f"{table_name}.geojson"
    assert geojson_path.exists()
    with open(geojson_path) as f:
        geojson_data = json.load(f)
        assert geojson_data["type"] == "FeatureCollection"
        assert len(geojson_data["features"]) == 2
        primary = next(
            f for f in geojson_data["features"] if f["id"] == PRIMARY_ENTITY_ID
        )
        assert primary["geometry"] == {
            "type": "Point",
            "coordinates": PRIMARY_COORDINATES,
        }

    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            assert cur.fetchone()[0] == 2

            cur.execute(
                f'SELECT g__type, g__coordinates, data_source, dataset_name, '
                f'"agentName", "beneficiariesTotal", "conceptLabels" '
                f"FROM {table_name} WHERE _id = %s",
                (PRIMARY_ENTITY_ID,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == "Point"
            assert json.loads(row[1]) == PRIMARY_COORDINATES
            assert row[2] == "SensingClues"
            assert row[3] == PRIMARY_PROJECT_NAME
            assert row[4] == PRIMARY_AGENT_NAME
            assert row[5] == PRIMARY_BENEFICIARIES_TOTAL
            assert json.loads(row[6]) == PRIMARY_CONCEPT_LABELS


def test_pagination(sensingclues_server_paginated, pg_database, tmp_path):
    asset_storage = tmp_path / "datalake"
    table_name = "sc_paginated"

    _run(
        sensingclues_server_paginated, pg_database, table_name, asset_storage
    )

    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            assert cur.fetchone()[0] == 3

            cur.execute(f"SELECT _id FROM {table_name}")
            ids = {row[0] for row in cur.fetchall()}
            assert ids == set(AFRICA_ENTITY_IDS)


def test_no_observations(sensingclues_server_empty, pg_database, tmp_path):
    asset_storage = tmp_path / "datalake"
    table_name = "sc_empty"

    _run(sensingclues_server_empty, pg_database, table_name, asset_storage)

    assert not (asset_storage / table_name / f"{table_name}.geojson").exists()

    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s)", (f"public.{table_name}",))
            assert cur.fetchone()[0] is None


def test_invalid_group(sensingclues_server_groups_only, pg_database, tmp_path):
    with pytest.raises(ValueError, match="999999"):
        _run(
            sensingclues_server_groups_only,
            pg_database,
            "sc_bad_group",
            tmp_path / "datalake",
            group_identifier="999999",
        )


def test_non_numeric_identifier(pg_database, tmp_path):
    with pytest.raises(ValueError, match="numeric"):
        main(
            "demo",
            "demo",
            "GFW",
            pg_database,
            "sc_bad_ident",
            attachment_root=tmp_path / "datalake",
        )


def test_invalid_credentials(sensingclues_server_unauthorized, pg_database, tmp_path):
    with pytest.raises(SCPermissionDenied):
        _run(
            sensingclues_server_unauthorized,
            pg_database,
            "sc_unauthorized",
            tmp_path / "datalake",
        )


def test_transform_collapses_concepts_and_flattens_attributes():
    raw = observations_page([CLUEY_GROUP])["results"]
    features = transform_observations_to_geojson(raw)["features"]
    primary = next(f for f in features if f["id"] == PRIMARY_ENTITY_ID)
    props = primary["properties"]

    assert props["conceptLabels"] == PRIMARY_CONCEPT_LABELS
    assert len(props["conceptIds"]) == len(PRIMARY_CONCEPT_LABELS)
    assert props["beneficiariesTotal"] == PRIMARY_BENEFICIARIES_TOTAL
    assert primary["geometry"] == {
        "type": "Point",
        "coordinates": PRIMARY_COORDINATES,
    }
    assert props["data_source"] == "SensingClues"
    assert props["dataset_name"] == PRIMARY_PROJECT_NAME


def test_transform_core_fields_win_on_attribute_collision():
    raw = observations_page([AFRICA_GROUP])["results"]
    features = transform_observations_to_geojson(raw)["features"]
    primary = next(f for f in features if f["id"] == AFRICA_PRIMARY_ENTITY_ID)
    props = primary["properties"]

    assert props["fileName"] == AFRICA_FILENAME
    assert props["ActionTaken"] == AFRICA_ACTION_TAKEN
    assert "attributes" not in props


def test_transform_missing_where_omits_geometry():
    result = transform_observations_to_geojson(
        [
            {
                "id": "O-no-geo",
                "extracted": {
                    "content": [
                        {
                            "headers": {
                                "entityId": "O-no-geo",
                                "projectId": "1",
                                "projectName": "No Geo",
                                "entityType": "animal",
                                "entityClass": "note",
                            }
                        },
                        {"info": {"title": "Observation"}},
                        {
                            "Observation": {
                                "observationType": "animal",
                                "observationClass": "note",
                                "when": "2024-01-01T00:00:00Z",
                                "concepts": [],
                                "attributes": [],
                                "agent": {"agentName": ""},
                            }
                        },
                    ]
                },
            }
        ]
    )
    feature = result["features"][0]
    assert feature["geometry"] is None
    assert "g__type" not in feature["properties"]
    assert "g__coordinates" not in feature["properties"]
