# twilio~=9.4

import json
import logging

from twilio.rest import Client as TwilioClient

# type names that refer to Windmill Resources
c_twilio_message_template = dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(
    alerts_statistics: dict,
    community_slug: str,
    twilio: c_twilio_message_template,
):
    send_twilio_message(twilio, alerts_statistics, community_slug)


def send_twilio_message(twilio, alerts_statistics, community_slug):
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
    client = TwilioClient(twilio["account_sid"], twilio["auth_token"])

    # Send a message to each recipient
    logger.info(
        f"Sending Twilio messages to {len(twilio.get('recipients', []))} recipients."
    )

    for recipient in twilio["recipients"]:
        client.messages.create(
            content_sid=twilio.get("content_sid"),
            content_variables=json.dumps(
                {
                    "1": alerts_statistics.get("total_alerts"),
                    "2": alerts_statistics.get("month_year"),
                    "3": alerts_statistics.get("description_alerts"),
                    "4": f"https://explorer.{community_slug}.guardianconnector.net/alerts/alerts",
                }
            ),
            messaging_service_sid=twilio.get("messaging_service_sid"),
            to=recipient,
            from_=twilio["origin_number"],
        )

    logger.info("Twilio messages sent successfully.")
