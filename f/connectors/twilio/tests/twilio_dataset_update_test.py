import json
import sys
import types

import pytest

from f.connectors.twilio.twilio_dataset_update import (
    build_dataset_update_message,
    main,
)

_TEMPLATE = {
    "account_sid": "AC",
    "auth_token": "token",
    "origin_number": "+10000000000",
    "recipients": ["+20000000000"],
    "content_sid": "HX",
    "messaging_service_sid": "MG",
}

_COMAPEO_MESSAGE = (
    "20 new CoMapeo observations were written to *comapeo_my_data*. "
    "Categories: animals, historical places, trees. "
    "View them here: https://explorer.springfield.guardianconnector.net/map/comapeo_my_data"
)


def _install_twilio(monkeypatch):
    sent = []

    class Messages:
        def create(self, **kwargs):
            sent.append(kwargs)

    class Client:
        def __init__(self, account_sid, auth_token):
            self.messages = Messages()

    twilio_rest = types.ModuleType("twilio.rest")
    twilio_rest.Client = Client
    twilio = types.ModuleType("twilio")
    twilio.rest = twilio_rest
    monkeypatch.setitem(sys.modules, "twilio", twilio)
    monkeypatch.setitem(sys.modules, "twilio.rest", twilio_rest)
    return sent


def test_comapeo_observations_message(monkeypatch):
    sent = _install_twilio(monkeypatch)

    message = main(
        data_statistics={
            "new_records": 20,
            "data_type": "CoMapeo observations",
            "filter_column": "categories",
            "list_of_values": ["animals", "historical places", "trees"],
        },
        instance_slug="springfield",
        db_table_name="comapeo_my_data",
        twilio_message_template=_TEMPLATE,
    )

    assert message == _COMAPEO_MESSAGE
    assert json.loads(sent[0]["content_variables"])["1"] == _COMAPEO_MESSAGE


@pytest.mark.parametrize(
    ("statistics", "instance_slug", "bbox", "expected"),
    [
        (
            {"new_records": 5},
            None,
            None,
            "5 new records were written to *table_a*.",
        ),
        (
            {"new_records": 5, "filter_column": "categories"},
            "springfield",
            None,
            "5 new records were written to *table_a*. "
            "View them here: https://explorer.springfield.guardianconnector.net/map/table_a",
        ),
        (
            {"new_records": 5, "list_of_values": ["roads"]},
            None,
            None,
            "5 new records were written to *table_a*.",
        ),
        (
            {"new_records": 4, "data_type": "features"},
            "springfield",
            [[-61.8, 17.3], [-61.5, 17.6]],
            "4 new features were detected within an area of interest of "
            "[[-61.8, 17.3], [-61.5, 17.6]] and written to *table_a*. "
            "View them here: https://explorer.springfield.guardianconnector.net/map/table_a",
        ),
        (
            {
                "new_records": 4,
                "data_type": "features",
                "filter_column": "kind",
                "list_of_values": "roads",
            },
            None,
            "[[-61.8, 17.3], [-61.5, 17.6]]",
            "4 new features were detected within an area of interest of "
            "[[-61.8, 17.3], [-61.5, 17.6]] and written to *table_a*. Kind: roads.",
        ),
    ],
)
def test_optional_clauses(statistics, instance_slug, bbox, expected):
    assert (
        build_dataset_update_message(statistics, "table_a", instance_slug, bbox)
        == expected
    )


def test_zero_new_records_skips_send(monkeypatch):
    sent = _install_twilio(monkeypatch)

    message = main(
        data_statistics={"new_records": 0, "data_type": "features"},
        db_table_name="gov_parcels",
        twilio_message_template=_TEMPLATE,
        bbox=[[-61.8, 17.3], [-61.5, 17.6]],
    )

    assert message is None
    assert sent == []


def test_missing_new_records_raises():
    with pytest.raises(ValueError, match="new_records"):
        build_dataset_update_message({}, "table_a")


@pytest.mark.parametrize(
    ("language", "expected"),
    [
        ("English", "5 new records were written to *table_a*."),
        ("Español", "5 registros nuevos fueron escritos en *table_a*."),
        ("Português", "5 registros novos foram escritos em *table_a*."),
        ("Nederlands", "5 nieuwe registraties zijn weggeschreven naar *table_a*."),
        ("Swahili", "5 rekodi mpya zimeandikwa kwenye *table_a*."),
        ("Thai", "5 รายการ ใหม่ถูกเขียนลงใน *table_a*."),
    ],
)
def test_language_translates_fixed_wording(language, expected):
    assert (
        build_dataset_update_message({"new_records": 5}, "table_a", language=language)
        == expected
    )


def test_language_translates_area_and_link():
    message = build_dataset_update_message(
        {"new_records": 4, "data_type": "features"},
        "gov_parcels",
        instance_slug="springfield",
        bbox=[[-61.8, 17.3], [-61.5, 17.6]],
        language="Español",
    )
    assert message == (
        "4 features nuevos fueron detectados dentro de un área de interés de "
        "[[-61.8, 17.3], [-61.5, 17.6]] y escritos en *gov_parcels*. "
        "Consúltalos aquí: https://explorer.springfield.guardianconnector.net/map/gov_parcels"
    )


def test_unknown_language_raises():
    with pytest.raises(ValueError, match="language must be one of"):
        build_dataset_update_message({"new_records": 1}, "table_a", language="French")
