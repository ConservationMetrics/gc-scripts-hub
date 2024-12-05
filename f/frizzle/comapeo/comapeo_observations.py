# requirements:
# psycopg2-binary
# requests~=2.32
# retrying~=1.3

import logging
import mimetypes
import os
import re

import psycopg2
from psycopg2 import errors
import requests

from retrying import retry

# type names that refer to Windmill Resources
postgresql = dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def conninfo(db: postgresql):
    """Convert a `postgresql` Windmill Resources to psycopg-style connection string"""
    return "dbname={dbname} user={user} password={password} host={host} port={port}".format(
        **db
    )


def main(
    comapeo_server_base_url: str,
    comapeo_access_token: str,
    comapeo_project_blocklist: list,
    db: postgresql,
    db_table_prefix: str = "comapeo",
    attachment_root: str = "/frizzle-persistent-storage/datalake",
):
    comapeo_projects = fetch_comapeo_projects(
        comapeo_server_base_url, comapeo_access_token, comapeo_project_blocklist
    )

    # Run culminates in success if there were no projects returned by the API
    if len(comapeo_projects) == 0:
        logger.info(
            "No projects fetched. Skipping data processing and database writing."
        )
        return

    logger.info(f"Fetched {len(comapeo_projects)} projects.")

    comapeo_data, attachment_failed = download_and_transform_comapeo_data(
        comapeo_server_base_url,
        comapeo_access_token,
        comapeo_projects,
        db_table_prefix,
        attachment_root,
    )
    logger.info(
        f"Downloaded {sum(len(observations) for observations in comapeo_data.values())} observations from {len(comapeo_data)} projects."
    )

    db_writer = CoMapeoDBWriter(conninfo(db))
    db_writer.handle_output(comapeo_data)

    logging.info(
        f"Wrote response content to database table(s) with prefix [{db_table_prefix}]"
    )

    if attachment_failed:
        raise RuntimeError("Some attachments failed to download.")


def fetch_comapeo_projects(
    comapeo_server_base_url, comapeo_access_token, comapeo_project_blocklist
):
    """
    Fetches a list of projects from the CoMapeo API, excluding any projects
    specified in the blocklist.

    Parameters
    ----------
    comapeo_server_base_url : str
        The base URL of the CoMapeo server.
    comapeo_access_token : str
        The access token used for authenticating with the CoMapeo API.
    comapeo_project_blocklist : list
        A list of project IDs to be excluded from the fetched results.

    Returns
    -------
    list
        A list of dictionaries, each containing the 'project_id' and 'project_name'
        of a project fetched from the CoMapeo API, excluding those in the blocklist.
    """

    url = f"{comapeo_server_base_url}/projects"
    headers = {"Authorization": f"Bearer {comapeo_access_token}"}
    payload = {}
    logger.info("Fetching projects from CoMapeo API...")
    response = requests.request("GET", url, headers=headers, data=payload)

    response.raise_for_status()
    results = response.json().get("data", [])

    comapeo_projects = [
        {
            "project_id": res.get("projectId"),
            "project_name": res.get("name"),
        }
        for res in results
    ]

    if comapeo_project_blocklist:
        logger.info(f"Blocked projects found: {comapeo_project_blocklist}")
        comapeo_projects = [
            project
            for project in comapeo_projects
            if project["project_id"] not in comapeo_project_blocklist
        ]

    return comapeo_projects


@retry(
    stop_max_attempt_number=3,
    wait_exponential_multiplier=500,
)
def download_attachment(url, headers, save_path):
    try:
        logger.info("I am trying to download this attachment...")
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        content_type = response.headers.get("Content-Type", "")
        extension = mimetypes.guess_extension(content_type) or ""

        file_name = os.path.basename(url) + extension
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        with open(save_path, "wb") as f:
            f.write(response.content)
        logger.info("Download completed.")
        return file_name

    except Exception as e:
        logger.error(f"Exception during download: {e}")
        return None


def camel_to_snake(name):
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()


def download_and_transform_comapeo_data(
    comapeo_server_base_url,
    comapeo_access_token,
    comapeo_projects,
    db_table_prefix,
    attachment_root,
):
    """
    Downloads and transforms CoMapeo project data from the API, converting it into a structured format and downloading any associated attachments.

    Parameters
    ----------
    comapeo_server_base_url : str
        The base URL of the CoMapeo server.
    comapeo_access_token : str
        The access token used for authenticating with the CoMapeo API.
    comapeo_projects : list
        A list of dictionaries, each containing 'project_id' and 'project_name' for the projects to be processed.
    db_table_prefix : str
        The prefix to be used for database table names.
    attachment_root : str
        The root directory where attachments will be saved.

    Returns
    -------
    tuple
        A tuple containing:
        - comapeo_data : dict
            A dictionary where keys are project names and values are lists of observations.
        - attachment_failed : bool
            A flag indicating if any attachment downloads failed.
    """

    comapeo_data = {}
    attachment_failed = False
    for index, project in enumerate(comapeo_projects):
        project_id = project["project_id"]
        project_name = project["project_name"]
        sanitized_project_name = re.sub(r"\W+", "_", project_name).lower()
        final_project_name = f"{db_table_prefix + '_' if db_table_prefix else ''}{sanitized_project_name}"

        # Download the project data
        url = f"{comapeo_server_base_url}/projects/{project_id}/observations"
        headers = {
            "Authorization": f"Bearer {comapeo_access_token}",
        }

        logger.info(f"Fetching project {index + 1} (ID: {project_id})...")
        response = requests.get(url, headers=headers)
        current_project_data = []

        try:
            current_project_data = response.json().get("data", [])
        except requests.exceptions.JSONDecodeError:
            logger.error("Failed to decode JSON from response.")
            logger.info("Response received: ", response.text)
            raise ValueError("Invalid JSON response received from server.")

        for observation in current_project_data:
            observation["project_name"] = project_name
            observation["project_id"] = project_id

            # Convert keys from camelCase to snake_case
            observation = {
                ("docId" if k == "docId" else camel_to_snake(k)): v
                for k, v in observation.items()
            }

            # Create g__coordinates field
            if "lat" in observation and "lon" in observation:
                observation["g__coordinates"] = (
                    f"[{observation['lon']}, {observation['lat']}]"
                )

            # Transform tags
            if "tags" in observation:
                for key, value in observation["tags"].items():
                    new_key = key.replace("-", "_")
                    observation[new_key] = value
                del observation["tags"]

            # Download attachments
            if "attachments" in observation:
                filenames = []
                for attachment in observation["attachments"]:
                    if "url" in attachment:
                        logger.info(attachment["url"])
                        file_name = download_attachment(
                            attachment["url"],
                            headers,
                            os.path.join(
                                attachment_root,
                                "comapeo",
                                sanitized_project_name,
                                "attachments",
                                os.path.basename(attachment["url"]),
                            ),
                        )
                        if file_name is not None:
                            filenames.append(file_name)
                        else:
                            logger.error(
                                f"Attachment download failed for URL: {attachment['url']}. Skipping attachment."
                            )
                            attachment_failed = True

                observation["attachments"] = ", ".join(filenames)

        # Store observations in a dictionary with project_id as key
        comapeo_data[final_project_name] = current_project_data

        logger.info(
            f"Project {index + 1} (ID: {project_id}, name: {project_name}): Processed {len(current_project_data)} observation(s)."
        )
    return comapeo_data, attachment_failed


class CoMapeoDBWriter:
    """
    Converts unstructured CoMapeo observations data to structured SQL tables.
    """

    def __init__(self, db_connection_string):
        """
        Initializes the CoMapeoIOManager with the provided connection string and form response table to be used.
        """
        self.db_connection_string = db_connection_string

    def _get_conn(self):
        """
        Establishes a connection to the PostgreSQL database using the class's configured connection string.
        """
        return psycopg2.connect(dsn=self.db_connection_string)

    def _inspect_schema(self, table_name):
        """
        Fetches the column names of the given table.
        """
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"
        )
        columns = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return columns

    def _table_exists(self, cursor, table_name):
        """
        Checks if the given table exists in the database.
        """
        cursor.execute(
            """
        SELECT EXISTS (
          SELECT FROM information_schema.tables 
          WHERE table_schema = 'public'
          AND table_name = %s
        );
        """,
            (table_name,),
        )
        return cursor.fetchone()[0]

    def _create_missing_fields(self, table_name, missing_columns):
        """
        Generates and executes SQL statements to add missing fields to the table.
        """
        try:
            with self._get_conn() as conn, conn.cursor() as cursor:
                # Check if the table exists and create it if it doesn't
                if not self._table_exists(cursor, table_name):
                    cursor.execute(f"""
                    CREATE TABLE public.{table_name} (
                        _id TEXT PRIMARY KEY);
                    """)
                    logger.info(f"Table {table_name} created.")

                for sanitized_column in missing_columns:
                    # it's pkey of the table
                    if sanitized_column == "_id":
                        continue
                    try:
                        cursor.execute(f"""
                        ALTER TABLE {table_name} 
                        ADD COLUMN "{sanitized_column}" TEXT;
                        """)
                    except errors.DuplicateColumn:
                        logger.error(
                            f"Skipping insert due to DuplicateColumn, this form column has been accounted for already in the past: {sanitized_column}"
                        )
                        continue
                    except Exception as e:
                        logger.error(
                            f"An error occurred while creating missing column: {sanitized_column} for {table_name}: {e}"
                        )
                        raise
        finally:
            conn.close()

    def handle_output(self, outputs):
        """
        Inserts CoMapeo project data into a PostgreSQL database. For each project, it checks the database schema and adds any missing fields. It then constructs and executes SQL insert queries to store the data in separate tables for each project. After processing all data, it commits the transaction and closes the database connection.
        """
        conn = self._get_conn()
        cursor = conn.cursor()

        comapeo_projects = outputs

        for project_name, project_data in comapeo_projects.items():
            existing_fields = self._inspect_schema(project_name)
            rows = []
            for entry in project_data:
                sanitized_entry = {
                    ("_id" if k == "docId" else k): v for k, v in entry.items()
                }
                rows.append(sanitized_entry)

            missing_field_keys = set()
            for row in rows:
                missing_field_keys.update(set(row.keys()).difference(existing_fields))

            if missing_field_keys:
                self._create_missing_fields(project_name, missing_field_keys)

            logger.info(f"Inserting {len(rows)} submissions into DB.")

            for row in rows:
                try:
                    # Wrap column names in single quotes
                    columns = ", ".join(f'"{name}"' for name in row.keys())

                    # Prepare the values list, converting None to 'NULL'
                    values = [
                        f"{value}" if value is not None else None
                        for value in row.values()
                    ]

                    # Construct the insert query
                    insert_query = f"INSERT INTO {project_name} ({columns}) VALUES ({', '.join(['%s'] * len(row))});"

                    cursor.execute(insert_query, tuple(values))
                except errors.UniqueViolation:
                    logger.debug(
                        f"Skipping insertion of rows to {project_name} due to UniqueViolation, this _id has been accounted for already in the past."
                    )
                    conn.rollback()
                    continue
                except Exception as e:
                    logger.error(f"Error inserting data: {e}, {type(e).__name__}")
                    conn.rollback()

                try:
                    conn.commit()
                except Exception as e:
                    logger.error(f"Error committing transaction: {e}")
                    conn.rollback()

        cursor.close()
        conn.close()
