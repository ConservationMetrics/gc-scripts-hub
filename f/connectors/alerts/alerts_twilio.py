# twilio~=9.4
# requests~=2.32.3

import json
import logging
from typing import TypedDict

from twilio.rest import Client as TwilioClient


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


def main(
    alerts_statistics: dict,
    community_slug: str,
    twilio_message_template: twilio_message_template,
):
    send_twilio_message(twilio_message_template, alerts_statistics, community_slug)


def send_twilio_message(twilio_message_template, alerts_statistics, community_slug):
    """
    Send a Twilio SMS message with alerts processing completion details.

    The message template is defined in the Twilio console, and is structured as follows:
    {{1}} new change detection alert(s) have been published on your alerts dashboard for
    the date of {{2}}. The following activities have been detected in your region: {{3}}.
    Visit your alerts dashboard here: {{4}}

    In the content_variables below, the placeholders {{1}}, {{2}}, {{3}}, and {{4}} are
    replaced with the corresponding values from the alerts_statistics dictionary.

    Parameters
    ----------
    twilio : dict
        A dictionary containing Twilio configuration parameters, including
        account credentials, messaging service details, and recipient phone
        numbers.
    alerts_statistics : dict
        A dictionary containing statistics about the processed alerts, such as
        the total number of alerts, month and year, and a description.
    community_slug : str
        The slug of the community for which alerts are being processed.
    """
    client = TwilioClient(
        twilio_message_template["account_sid"], twilio_message_template["auth_token"]
    )

    # Send a message to each recipient
    logger.info(
        f"Sending Twilio messages to {len(twilio_message_template.get('recipients', []))} recipients."
    )

    for recipient in twilio_message_template["recipients"]:
        client.messages.create(
            content_sid=twilio_message_template.get("content_sid"),
            content_variables=json.dumps(
                {
                    "1": alerts_statistics.get("total_alerts"),
                    "2": alerts_statistics.get("month_year"),
                    "3": alerts_statistics.get("description_alerts"),
                    "4": f"https://explorer.{community_slug}.guardianconnector.net/alerts/alerts",
                }
            ),
            messaging_service_sid=twilio_message_template.get("messaging_service_sid"),
            to=recipient,
            from_=twilio_message_template["origin_number"],
        )

    logger.info("Twilio messages sent successfully.")
