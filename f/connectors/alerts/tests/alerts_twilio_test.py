import json
import sys
import types

from f.connectors.alerts.alerts_twilio import main


def test_schedule_payload_keeps_db_table_name(monkeypatch):
    """Windmill schedules store inputs by argument name."""
    sent = {}

    class Messages:
        def create(self, **kwargs):
            sent.update(kwargs)

    class Client:
        def __init__(self, account_sid, auth_token):
            self.messages = Messages()

    twilio_rest = types.ModuleType("twilio.rest")
    twilio_rest.Client = Client
    twilio = types.ModuleType("twilio")
    twilio.rest = twilio_rest
    monkeypatch.setitem(sys.modules, "twilio", twilio)
    monkeypatch.setitem(sys.modules, "twilio.rest", twilio_rest)

    main(
        alerts_statistics={
            "total_alerts": 2,
            "date": "2026-01",
            "description_alerts": "logging",
        },
        instance_slug="demo",
        db_table_name="observations",
        twilio_message_template={
            "account_sid": "AC",
            "auth_token": "token",
            "origin_number": "+10000000000",
            "recipients": ["+20000000000"],
            "content_sid": "HX",
            "messaging_service_sid": "MG",
        },
    )

    assert json.loads(sent["content_variables"])["4"].endswith("/alerts/observations")
