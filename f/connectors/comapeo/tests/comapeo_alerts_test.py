from typing import NamedTuple

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
        Alert("abc123", "Hello, world!"),
        Alert("def456", "Goodbye, world!"),
    ]

    with pg_database.cursor() as cur:
        cur.execute("""
            CREATE TEMPORARY TABLE fake_alerts (
                alert_id TEXT PRIMARY KEY,
                alert_message TEXT
            )
        """)

        values = [(a.alert_id, a.alert_message) for a in alerts]
        cur.executemany("INSERT INTO fake_alerts VALUES (%s, %s)", values)

    return alerts


def test_script_e2e(comapeoserver_alerts, pg_database, fake_alerts_table):
    main(
        pg_database,
        comapeoserver_alerts.comapeo_server,
        "forest_expedition",
        "fake_alerts",
    )

    expected_alerts = set(a.alert_id for a in fake_alerts_table)
    assert expected_alerts == set(comapeoserver_alerts.posted_alerts)
