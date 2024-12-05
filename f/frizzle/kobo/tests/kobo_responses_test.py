import re
from dataclasses import dataclass

import pytest

from f.frizzle.kobo.kobo_responses import main, sanitize
from f.frizzle.kobo.tests import kobo_form


def test_sanitize():
    message = {
        "col.1": 1,
        "col?2": 2,
        "Cultura/col3": 3,
        "col_002": 4,
        "x": 5,
    }
    global_mapping = {"x": "X"}

    sql_message, updated_global_mapping = sanitize(
        message, global_mapping, "/", [("/", "__")]
    )
    assert sql_message == {
        "col1": 1,
        "col2": 2,
        "col3__Cultura": 3,
        "col_002": 4,
        "X": 5,
    }
    assert updated_global_mapping == {
        "x": "X",
        "col.1": "col1",
        "col?2": "col2",
        "Cultura/col3": "col3__Cultura",
        "col_002": "col_002",
    }


def test_sanitize__same_letters():
    message = {"foo[bar]test": 1, "foo{bar}test": 2}

    sql_message, _ = sanitize(message, {}, maxlen=12)
    assert sql_message == {
        "foobartest": 1,
        "foobarte_001": 2,
    }


def test_sanitize_column_names__long():
    message = {
        "column.1": 1,
        "column12": 2,
        "column13": 3,
        "col_002": 4,
        "x": 5,
    }

    sql_message, updated_global_mapping = sanitize(message, {}, maxlen=7)
    print(sql_message)
    assert sql_message == {
        "column1": 1,
        "col_001": 2,
        "col_002": 3,  # Note that column13 got assigned another actual column's name!
        "col_003": 4,  # Note that col_002 got renamed to col_003!
        "x": 5,
    }
    assert updated_global_mapping == {
        "column.1": "column1",
        "column12": "col_001",
        "column13": "col_002",  # !!
        "col_002": "col_003",  # !!
        "x": "x",
    }


def test_sanitize_with_nesting():
    """sanitize() JSON-serializes deeply nested types."""
    message = {
        "group1": {"group2": {"question": "How ya doin?"}},
        "url": "gopher://example.net",
    }

    sql_message, _ = sanitize(message, {})
    assert sql_message == {
        "group1": '{"group2": {"question": "How ya doin?"}}',
        "url": "gopher://example.net",
    }


@pytest.fixture
def koboserver(mocked_responses):
    """A mock Kobo Server that you can use to provide survey responses"""

    @dataclass
    class KoboServer:
        account: dict
        form_id: str

    server_url = "http://kobo.example.org"
    form_id = "mimsyweretheborogoves"
    form_name = "Arboles"

    mocked_responses.get(
        f"{server_url}/api/v2/assets/{form_id}/",
        json=kobo_form.kobo_form(server_url, form_id, form_name),
        status=200,
    )
    mocked_responses.get(
        f"{server_url}/api/v2/assets/{form_id}/data/",
        json=kobo_form.kobo_form_submissions(server_url, form_id),
        status=200,
    )
    mocked_responses.get(
        re.compile(rf"{server_url}/api/v2/assets/{form_id}/data/\d+/attachments/\d+/?"),
        body=open("f/frizzle/kobo/tests/trees.png", "rb").read(),
        content_type="image/png",
        headers={"Content-Length": "3632"},
    )

    return KoboServer(dict(server_url=server_url, api_key="Callooh!Callay!"), form_id)


@pytest.fixture
def pg_database():
    return dict(dbname="", user="", password="", host="", port="")


def test_script(koboserver, pg_database, tmp_path):
    attachments = tmp_path / "attachments"

    main(
        koboserver.account, koboserver.form_id, pg_database, "kobo_responses", tmp_path
    )

    # Attachments are saved to disk
    assert (
        attachments
        / "cmi_admin_kobo_test/attachments/f7bef041e8624f09946bff05ee5cbd4b/5c408d9d-6a76-4fbb-bf4e-9ed8f8e3e382/1637241249813.jpg"
    ).exists()

    # # Survey responses are written to a SQL Table
    # engine = sqlalchemy.create_engine(f"sqlite:///{warehousedb}")
    # assert sqlalchemy.inspect(engine).has_table("kobo_responses")
