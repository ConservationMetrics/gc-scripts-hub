# requirements:
# psycopg2-binary
# requests~=2.32

import logging
import os
import re

import psycopg2
import requests
from psycopg2 import errors

import mimetypes

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

    comapeo_data = process_comapeo_data(
        comapeo_server_base_url, comapeo_access_token, comapeo_projects, attachment_root
    )
    logger.info(
        f"Downloaded {sum(len(observations) for observations in comapeo_data.values())} observations from {len(comapeo_data)} projects."
    )

    # TODO: During data processing, ensure table names are sanitized and constructed using db_table_prefix and project names, rather than delegating this task to the DB writer.

    db_writer = CoMapeoDBWriter(conninfo(db), db_table_prefix)
    db_writer.handle_output(comapeo_data)

    logging.info(
        f"Wrote response content to database table(s) with prefix [{db_table_prefix}]"
    )


def fetch_comapeo_projects(
    comapeo_server_base_url, comapeo_access_token, comapeo_project_blocklist
):
    url = f"{comapeo_server_base_url}/projects"
    headers = {"Authorization": f"Bearer {comapeo_access_token}"}
    payload = {}
    logger.info("Fetching projects from CoMapeo API...")
    response = requests.request("GET", url, headers=headers, data=payload)

    if response.status_code != 200:
        logger.error(f"Unexpected status code: {response.status_code}")
        raise ValueError(
            f"Failed to fetch projects: {response.status_code} - {response.reason}"
        )

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


def process_comapeo_data(
    comapeo_server_base_url, comapeo_access_token, comapeo_projects, attachment_root
):
    comapeo_data = {}
    for index, project in enumerate(comapeo_projects):
        project_id = project["project_id"]
        project_name = project["project_name"]
        sanitized_project_name = re.sub(r"\W+", "_", project_name).lower()

        # Download the project data
        url = f"{comapeo_server_base_url}/projects/{project_id}/observations"
        headers = {
            "Authorization": f"Bearer {comapeo_access_token}",
        }

        logger.info(f"Fetching project {index + 1} (ID: {project_id})...")
        response = requests.get(url, headers=headers)
        current_project_data = []

        # TODO: add retries
        try:
            current_project_data = response.json().get("data", [])
        except requests.exceptions.JSONDecodeError:
            logger.error("Failed to decode JSON from response.")
            logger.info("Response received: ", response.text)
            raise ValueError("Invalid JSON response received from server.")

        for observation in current_project_data:
            observation["project_name"] = project_name
            observation["project_id"] = project_id

            # Create g__coordinates field
            if "lat" in observation and "lon" in observation:
                observation["g__coordinates"] = (
                    f"[{observation['lon']}, {observation['lat']}]"
                )

            # Process tags
            if "tags" in observation:
                for key, value in observation["tags"].items():
                    new_key = key.replace("-", "_")
                    observation[new_key] = value
                del observation["tags"]

            # Process attachments
            if "attachments" in observation:
                filenames = []
                for attachment in observation["attachments"]:
                    if "url" in attachment:
                        logger.info(attachment["url"])
                        response = requests.get(attachment["url"], headers=headers)
                        if response.status_code == 200:
                            content_type = response.headers.get("Content-Type", "")
                            extension = mimetypes.guess_extension(content_type) or ""

                            file_name = os.path.basename(attachment["url"]) + extension
                            filenames.append(file_name)

                            save_path = os.path.join(
                                attachment_root,
                                "comapeo",
                                sanitized_project_name,
                                "attachments",
                                file_name,
                            )
                            os.makedirs(os.path.dirname(save_path), exist_ok=True)
                            with open(save_path, "wb") as file:
                                file.write(response.content)
                            logger.info("Download completed.")
                        else:
                            # TODO: add retries
                            try:
                                error_message = response.json().get(
                                    "message", "Unknown error"
                                )
                            except requests.exceptions.JSONDecodeError:
                                error_message = "No JSON response received."
                            logger.error(
                                f"Failed downloading attachments. Error: {error_message}"
                            )

                observation["attachments"] = ", ".join(filenames)

        # Store observations in a dictionary with project_id as key
        comapeo_data[sanitized_project_name] = current_project_data

        logger.info(
            f"Project {index + 1} (ID: {project_id}, name: {project_name}): Processed {len(current_project_data)} observation(s)."
        )
    return comapeo_data


def camel_to_snake(name):
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()


class CoMapeoDBWriter:
    """
    Converts unstructured CoMapeo observations data to structured SQL tables.
    """

    def __init__(self, db_connection_string, table_prefix):
        """
        Initializes the CoMapeoIOManager with the provided connection string and form response table to be used.
        """
        self.db_connection_string = db_connection_string
        self.table_prefix = table_prefix

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
        Processes CoMapeo data and inserts it into a PostgreSQL database. It iterates over each CoMapeo object, extracts relevant features and properties, and constructs SQL queries to insert these data into the database. After processing all features, it commits the transaction and closes the database connection.
        """
        table_prefix = self.table_prefix

        conn = self._get_conn()
        cursor = conn.cursor()

        comapeo_projects = outputs

        for project_name, project_data in comapeo_projects.items():
            sanitized_project_name = re.sub(r"\W+", "_", project_name).lower()
            table_name = f"{table_prefix}_{sanitized_project_name}"

            project_data = [
                {
                    ("docId" if k == "docId" else camel_to_snake(k)): v
                    for k, v in entry.items()
                }
                for entry in project_data
            ]

            existing_fields = self._inspect_schema(table_name)
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
                self._create_missing_fields(table_name, missing_field_keys)

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
                    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({', '.join(['%s'] * len(row))});"

                    cursor.execute(insert_query, tuple(values))
                except errors.UniqueViolation:
                    logger.debug(
                        f"Skipping insertion of rows to {table_name} due to UniqueViolation, this _id has been accounted for already in the past."
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
