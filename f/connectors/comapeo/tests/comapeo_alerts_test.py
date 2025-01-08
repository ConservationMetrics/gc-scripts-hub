from typing import NamedTuple

import psycopg2
import pytest

from f.connectors.comapeo.comapeo_alerts import (
    main,
)


class Alert(NamedTuple):
    alert_id: str
    alert_message: str


@pytest.fixture
def fake_alerts_table(pg_database):
    alerts = [
        Alert("abc123", "gold_mining"),
        Alert("def456", "illegal_fishing"),
    ]

    conn = psycopg2.connect(**pg_database)
    conn.autocommit = True
    cur = conn.cursor()

    try:
        cur.execute("""
            CREATE TABLE fake_alerts (
                alert_id TEXT PRIMARY KEY,
                alert_type TEXT
            )
        """)  # there are more alert columns than these, but it is not necessary for this test to include them

        values = [(a.alert_id, a.alert_message) for a in alerts]
        cur.executemany("INSERT INTO fake_alerts VALUES (%s, %s)", values)

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
    assert expected_alerts == {"def456", "abc123"}
