# twilio~=9.4
# requests~=2.32.3

import json
import logging
from typing import TypedDict

from f.common_logic.db_operations import (
    USE_EXISTING_DATASET,
    DynSelect_db_table_name,
    postgresql,
    resolve_db_table_name,
)
from f.common_logic.db_operations import (
    existing_db_table_name as list_dataset_tables,
)


# https://hub.windmill.dev/resource_types/274/twilio_message_template
class twilio_message_template(TypedDict):
    account_sid: str
    auth_token: str
    origin_number: str
    recipients: list[str]
    content_sid: str
    messaging_service_sid: str


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Fixed wording only. Caller-supplied data_type, column names, and values pass through as given.
_PHRASES = {
    "English": {
        "records": "records",
        "written": "{count} new {data_type} were written to {table}.",
        "area": (
            "{count} new {data_type} were detected within an area of interest "
            "of {area} and written to {table}."
        ),
        "view": "View them here: {url}",
    },
    "Español": {
        "records": "registros",
        "written": "{count} {data_type} nuevos fueron escritos en {table}.",
        "area": (
            "{count} {data_type} nuevos fueron detectados dentro de un área de "
            "interés de {area} y escritos en {table}."
        ),
        "view": "Consúltalos aquí: {url}",
    },
    "Português": {
        "records": "registros",
        "written": "{count} {data_type} novos foram escritos em {table}.",
        "area": (
            "{count} {data_type} novos foram detectados dentro de uma área de "
            "interesse de {area} e escritos em {table}."
        ),
        "view": "Veja-os aqui: {url}",
    },
    "Nederlands": {
        "records": "registraties",
        "written": "{count} nieuwe {data_type} zijn weggeschreven naar {table}.",
        "area": (
            "{count} nieuwe {data_type} zijn gedetecteerd binnen een "
            "interessegebied van {area} en weggeschreven naar {table}."
        ),
        "view": "Bekijk ze hier: {url}",
    },
    "Swahili": {
        "records": "rekodi",
        "written": "{count} {data_type} mpya zimeandikwa kwenye {table}.",
        "area": (
            "{count} {data_type} mpya zimegunduliwa ndani ya eneo la maslahi "
            "la {area} na kuandikwa kwenye {table}."
        ),
        "view": "Zitazame hapa: {url}",
    },
    "Thai": {
        "records": "รายการ",
        "written": "{count} {data_type} ใหม่ถูกเขียนลงใน {table}.",
        "area": (
            "{count} {data_type} ใหม่ถูกตรวจพบในพื้นที่ที่สนใจ {area} และถูกเขียนลงใน {table}."
        ),
        "view": "ดูได้ที่นี่: {url}",
    },
}


def db_table_name(db: postgresql | None = None):
    """Windmill dynamic select. Only `db` is an argument, so the picker does not wait on the other fields."""
    return list_dataset_tables(db)


def _initcap_first_word(text: str) -> str:
    first, space, rest = text.partition(" ")
    return first[:1].upper() + first[1:] + space + rest


def _plain(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _format_values(list_of_values) -> str | None:
    if isinstance(list_of_values, (list, tuple)):
        parts = [text for item in list_of_values if (text := _plain(item))]
        return ", ".join(parts) or None
    return _plain(list_of_values)


def _format_bbox(bbox) -> str | None:
    """Return the bbox text for "an area of interest of <bbox>"."""
    if bbox is None or bbox == "" or bbox == []:
        return None
    parsed = bbox
    if isinstance(bbox, str):
        text = bbox.strip()
        if not text:
            return None
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return text
    if isinstance(parsed, (list, tuple)):
        return json.dumps(parsed)
    return _plain(parsed)


def _phrases(language: str | None) -> dict[str, str]:
    selected = language or "English"
    phrases = _PHRASES.get(selected)
    if phrases is None:
        choices = ", ".join(_PHRASES)
        raise ValueError(f"language must be one of: {choices}.")
    return phrases


def build_dataset_update_message(
    data_statistics: dict,
    db_table_name: str,
    instance_slug: str | None = None,
    bbox: str | list | None = None,
    language: str | None = None,
) -> str:
    """Build the WhatsApp body for one dataset write.

    The Twilio content template is ``{{1}}`` plus a fixed footer. This function
    fills ``{{1}}``. Optional clauses are left out here. ``language`` selects
    the fixed wording. A missing ``data_type`` uses that language's word for
    "records". ``filter_column`` and ``list_of_values`` are included only when
    both are set. The map link is included only when ``instance_slug`` is set.
    ``bbox`` switches the first sentence to the area-of-interest form.
    """
    new_records = data_statistics.get("new_records")
    if new_records is None:
        raise ValueError("data_statistics.new_records is required.")

    phrases = _phrases(language)
    data_type = _plain(data_statistics.get("data_type")) or phrases["records"]
    area = _format_bbox(bbox)
    fields = {
        "count": new_records,
        "data_type": data_type,
        "table": f"*{db_table_name}*",
        "area": area,
    }
    message = phrases["area" if area else "written"].format(**fields)

    filter_column = _plain(data_statistics.get("filter_column"))
    values = _format_values(data_statistics.get("list_of_values"))
    if filter_column and values:
        message += f" {_initcap_first_word(filter_column)}: {values}."

    slug = _plain(instance_slug)
    if slug:
        url = f"https://explorer.{slug}.guardianconnector.net/map/{db_table_name}"
        message += f" {phrases['view'].format(url=url)}"
    return message


def main(
    data_statistics: dict,
    db_table_name: DynSelect_db_table_name,
    twilio_message_template: twilio_message_template,
    db: postgresql | None = None,
    instance_slug: str | None = None,
    bbox: str | list | None = None,
    language: str = "English",
):
    if isinstance(db, dict):
        db_table_name = resolve_db_table_name(
            db, USE_EXISTING_DATASET, selected_table=db_table_name
        )
    if data_statistics.get("new_records") == 0:
        logger.info("No new records to report. Skipping Twilio message.")
        return None

    message = build_dataset_update_message(
        data_statistics, db_table_name, instance_slug, bbox, language
    )
    send_twilio_message(twilio_message_template, message)
    return message


def send_twilio_message(twilio_message_template, message: str):
    """Send ``message`` as content variable ``{{1}}`` to each recipient."""
    from twilio.rest import Client as TwilioClient

    client = TwilioClient(
        twilio_message_template["account_sid"], twilio_message_template["auth_token"]
    )
    content_variables = json.dumps({"1": message})
    recipients = twilio_message_template.get("recipients", [])
    logger.info(f"Sending Twilio messages to {len(recipients)} recipients.")

    for recipient in recipients:
        client.messages.create(
            content_sid=twilio_message_template.get("content_sid"),
            content_variables=content_variables,
            messaging_service_sid=twilio_message_template.get("messaging_service_sid"),
            to=recipient,
            from_=twilio_message_template["origin_number"],
        )

    logger.info("Twilio messages sent successfully.")
