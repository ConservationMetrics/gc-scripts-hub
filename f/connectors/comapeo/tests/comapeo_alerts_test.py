from typing import NamedTuple

import psycopg
import pytest

from f.connectors.comapeo.comapeo_alerts import (
    main,
)


class Alert(NamedTuple):
    alert_id: str
    alert_type: str
    g__type: str
    g__coordinates: tuple[float, float]
    date_start_t0: str
    date_end_t0: str


@pytest.fixture
def fake_alerts_table(pg_database):
    alerts = [
        Alert(
            "abc123", "gold_mining", "Point", "[12.0, 34.0]", "2023-01-01", "2023-01-02"
        ),
        Alert(
            "def456",
            "illegal_fishing",
            "Point",
            "[56.0, 78.0]",
            "2023-02-01",
            "2023-02-02",
        ),
    ]

    conn = psycopg.connect(autocommit=True, **pg_database)
    cur = conn.cursor()

    try:
        cur.execute("""
            CREATE TABLE fake_alerts (
                alert_id TEXT PRIMARY KEY,
                alert_type TEXT,
                g__type TEXT,
                g__coordinates TEXT,
                date_start_t0 TEXT,
                date_end_t0 TEXT
            )
        """)

        values = [
            (
                a.alert_id,
                a.alert_type,
                a.g__type,
                a.g__coordinates,
                a.date_start_t0,
                a.date_end_t0,
            )
            for a in alerts
        ]
        cur.executemany(
            "INSERT INTO fake_alerts VALUES (%s, %s, %s, %s, %s, %s)", values
        )

        yield alerts
    finally:
        cur.close()
        conn.close()


def test_script_e2e(comapeoserver_alerts, pg_database, fake_alerts_table):
    main(
        pg_database,
        comapeoserver_alerts.comapeo_server,
        "forest_expedition",
        "fake_alerts",
    )

    expected_alerts = set(a.alert_id for a in fake_alerts_table)
    assert expected_alerts == {
        "def456",
        "abc123",
    }  # abc123 already exists on the CoMapeo server
