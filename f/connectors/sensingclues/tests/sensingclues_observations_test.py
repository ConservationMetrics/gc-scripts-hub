import json

import psycopg
import pytest
from sensingcluespy.src.exceptions import SCPermissionDenied

from f.connectors.sensingclues.sensingclues_observations import (
    main,
    transform_observations,
)
from f.connectors.sensingclues.tests.assets.server_responses import (
    AFRICA_ACTION_TAKEN,
    AFRICA_ENTITY_IDS,
    AFRICA_FILENAME,
    AFRICA_GROUP,
    AFRICA_PRIMARY_ENTITY_ID,
    AFRICA_PROJECT_NAME,
    CLUEY_GROUP,
    PRIMARY_AGENT_NAME,
    PRIMARY_BENEFICIARIES_TOTAL,
    PRIMARY_CONCEPT_LABELS,
    PRIMARY_COORDINATES,
    PRIMARY_ENTITY_ID,
    PRIMARY_PROJECT_NAME,
    observations_page,
)


def _run(server, db, table_name, attachment_root, groups=None):
    main(
        server.username,
        server.password,
        groups if groups is not None else server.groups,
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

    csv_path = asset_storage / table_name / f"{table_name}.csv"
    assert csv_path.exists()

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


def test_multiple_groups(sensingclues_server_both_groups, pg_database, tmp_path):
    asset_storage = tmp_path / "datalake"
    table_name = "sc_both"

    _run(
        sensingclues_server_both_groups, pg_database, table_name, asset_storage
    )

    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            assert cur.fetchone()[0] == 5

            cur.execute(
                f"SELECT dataset_name FROM {table_name} WHERE _id = %s",
                (PRIMARY_ENTITY_ID,),
            )
            assert cur.fetchone()[0] == PRIMARY_PROJECT_NAME

            cur.execute(
                f"SELECT dataset_name FROM {table_name} WHERE _id = %s",
                (AFRICA_PRIMARY_ENTITY_ID,),
            )
            assert cur.fetchone()[0] == AFRICA_PROJECT_NAME


def test_no_observations(sensingclues_server_empty, pg_database, tmp_path):
    asset_storage = tmp_path / "datalake"
    table_name = "sc_empty"

    _run(sensingclues_server_empty, pg_database, table_name, asset_storage)

    assert not (asset_storage / table_name / f"{table_name}.csv").exists()

    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s)", (f"public.{table_name}",))
            assert cur.fetchone()[0] is None


def test_invalid_group(sensingclues_server_groups_only, pg_database, tmp_path):
    with pytest.raises(ValueError, match="focus-project-does-not-exist"):
        _run(
            sensingclues_server_groups_only,
            pg_database,
            "sc_bad_group",
            tmp_path / "datalake",
            groups=["focus-project-does-not-exist"],
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
    rows = transform_observations(raw)
    primary = next(row for row in rows if row["_id"] == PRIMARY_ENTITY_ID)

    assert primary["conceptLabels"] == PRIMARY_CONCEPT_LABELS
    assert len(primary["conceptIds"]) == len(PRIMARY_CONCEPT_LABELS)
    assert primary["beneficiariesTotal"] == PRIMARY_BENEFICIARIES_TOTAL
    assert primary["g__type"] == "Point"
    assert primary["g__coordinates"] == PRIMARY_COORDINATES
    assert primary["data_source"] == "SensingClues"
    assert primary["dataset_name"] == PRIMARY_PROJECT_NAME


def test_transform_core_fields_win_on_attribute_collision():
    raw = observations_page([AFRICA_GROUP])["results"]
    rows = transform_observations(raw)
    primary = next(row for row in rows if row["_id"] == AFRICA_PRIMARY_ENTITY_ID)

    assert primary["fileName"] == AFRICA_FILENAME
    assert primary["ActionTaken"] == AFRICA_ACTION_TAKEN
    assert "attributes" not in primary


def test_transform_missing_where_omits_geometry():
    rows = transform_observations(
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
    assert "g__type" not in rows[0]
    assert "g__coordinates" not in rows[0]
