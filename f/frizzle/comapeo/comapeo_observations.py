# requirements:
# psycopg2-binary
# requests~=2.32

import json
import logging
import mimetypes
import os
import re
from typing import TypedDict

import psycopg2
import requests
from psycopg2 import errors, sql

# type names that refer to Windmill Resources
postgresql = dict


class comapeo_server(TypedDict):
    server_url: str
    access_token: str


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def conninfo(db: postgresql):
    """Convert a `postgresql` Windmill Resources to psycopg-style connection string"""
    # password is optional
    password_part = f" password={db['password']}" if "password" in db else ""
    conn = "dbname={dbname} user={user} host={host} port={port}".format(**db)
    return conn + password_part


def main(
    comapeo: comapeo_server,
    comapeo_project_blocklist: list,
    db: postgresql,
    db_table_prefix: str = "comapeo",
    attachment_root: str = "/frizzle-persistent-storage/datalake",
):
    server_url = comapeo["server_url"]
    access_token = comapeo["access_token"]

    comapeo_projects = fetch_comapeo_projects(
        server_url, access_token, comapeo_project_blocklist
    )

    # Run culminates in success if there were no projects returned by the API
    if len(comapeo_projects) == 0:
        logger.info(
            "No projects fetched. Skipping data processing and database writing."
        )
        return

    logger.info(f"Fetched {len(comapeo_projects)} projects.")

    comapeo_data, attachment_failed = download_and_transform_comapeo_data(
        server_url,
        access_token,
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


def fetch_comapeo_projects(server_url, access_token, comapeo_project_blocklist):
    """
    Fetches a list of projects from the CoMapeo API, excluding any projects
    specified in the blocklist.

    Parameters
    ----------
    server: comapeo_server
        For authenticating with the CoMapeo API
    comapeo_project_blocklist : list
        A list of project IDs to be excluded from the fetched results.

    Returns
    -------
    list
        A list of dictionaries, each containing the 'project_id' and 'project_name'
        of a project fetched from the CoMapeo API, excluding those in the blocklist.
    """

    url = f"{server_url}/projects"
    headers = {"Authorization": f"Bearer {access_token}"}
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


def download_attachment(url, headers, save_path):
    """
    Downloads a file from a specified URL and saves it to a given path.

    Parameters
    ----------
    url : str
        The URL of the file to be downloaded.
    headers : dict
        A dictionary of HTTP headers to send with the request, such as authentication tokens.
    save_path : str
        The file system path where the downloaded file will be saved.

    Returns
    -------
    str or None
        The name of the file if the download is successful, or None if an error occurs.

    Notes
    -----
    The function attempts to determine the file extension based on the 'Content-Type'
    header of the HTTP response from the CoMapeo Server. If the 'Content-Type' is not recognized,
    the file will be saved without an extension.

    The function intentionally does not raise exceptions. Instead, it logs errors and returns None,
    allowing the caller to handle the download failure gracefully.
    """
    try:
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


def normalize_and_snakecase_keys(dictionary):
    """
    Converts the keys of a dictionary from camelCase to snake_case, handling key collisions and truncating long keys.

    Parameters
    ----------
    dictionary : dict
        The dictionary whose keys are to be converted.

    Returns
    -------
    dict
        A new dictionary with the keys converted to snake_case and truncated if necessary.
    """
    new_dict = {}
    items = list(dictionary.items())
    for key, value in items:
        new_key = re.sub("([a-z0-9])([A-Z])", r"\1_\2", key).replace("-", "_").lower()

        base_key = new_key[:61] if len(new_key) > 63 else new_key

        final_key = base_key
        if len(new_key) > 63:
            final_key = f"{base_key}_1"

        counter = 1
        while final_key in new_dict:
            counter += 1
            final_key = f"{base_key}_{counter}"

        new_dict[final_key] = value
    return new_dict


def download_and_transform_comapeo_data(
    server_url,
    access_token,
    comapeo_projects,
    db_table_prefix,
    attachment_root,
):
    """
    Downloads and transforms CoMapeo project data from the API, converting it into a structured format and downloading any associated attachments.

    Parameters
    ----------
    comapeo_server : dict
        A dictionary containing the 'server_url' and 'access_token' keys for the CoMapeo server.
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
        url = f"{server_url}/projects/{project_id}/observations"
        headers = {
            "Authorization": f"Bearer {access_token}",
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

        for i, observation in enumerate(current_project_data):
            observation["project_name"] = project_name
            observation["project_id"] = project_id

            # Create k/v pairs for each tag
            if "tags" in observation:
                for key, value in observation["tags"].items():
                    observation[key] = value
                del observation["tags"]

            # Convert keys from camelCase to snake_case, handling key collisions and char limits
            observation = normalize_and_snakecase_keys(observation)

            # Create g__coordinates and g__type fields
            # Currently, only Point observations with lat and lon fields are returned by the CoMapeo API
            # Other geometry types and formats may be added in the future
            if "lat" in observation and "lon" in observation:
                observation["g__coordinates"] = (
                    f"[{observation['lon']}, {observation['lat']}]"
                )
                observation["g__type"] = "Point"

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

            current_project_data[i] = observation

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
            "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
            (table_name,),
        )
        columns = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return columns

    def _create_missing_fields(self, table_name, missing_columns):
        """
        Generates and executes SQL statements to add missing fields to the table.
        """
        table_name = sql.Identifier(table_name)
        try:
            with self._get_conn() as conn, conn.cursor() as cursor:
                query = sql.SQL(
                    "CREATE TABLE IF NOT EXISTS {table_name} (_id TEXT PRIMARY KEY);"
                ).format(table_name=table_name)
                cursor.execute(query)

                for sanitized_column in missing_columns:
                    if sanitized_column == "_id":
                        continue
                    try:
                        query = sql.SQL(
                            "ALTER TABLE {table_name} ADD COLUMN {colname} TEXT;"
                        ).format(
                            table_name=table_name,
                            colname=sql.Identifier(sanitized_column),
                        )
                        cursor.execute(query)
                    except errors.DuplicateColumn:
                        logger.debug(
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

    @staticmethod
    def _safe_insert(cursor, table_name, columns, values):
        """
        Safely construct and execute an INSERT query to avoid SQL injection.

        Parameters
        ----------
        cursor : psycopg2 cursor
            The database cursor for executing the query.
        table_name : str
            The name of the table to insert data into.
        columns : list of str
            The list of column names to insert data into.
        values : list
            The values to insert into the table.

        Returns
        -------
        None
        """
        query = sql.SQL(
            "INSERT INTO {table} ({fields}) VALUES ({placeholders})"
        ).format(
            table=sql.Identifier(table_name),
            fields=sql.SQL(", ").join(map(sql.Identifier, columns)),
            placeholders=sql.SQL(", ").join(sql.Placeholder() for _ in values),
        )

        cursor.execute(query, values)

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
                    ("_id" if k == "doc_id" else k): v for k, v in entry.items()
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
                    cols, vals = zip(*row.items())

                    # Serialize lists, dict values to JSON text
                    vals = list(vals)
                    for i in range(len(vals)):
                        value = vals[i]
                        if isinstance(value, list) or isinstance(value, dict):
                            vals[i] = json.dumps(value)

                    self._safe_insert(cursor, project_name, cols, vals)
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
