# requirements:
# psycopg2-binary
# requests~=2.32

import json
import logging
import os
import re
import time

import psycopg2
import requests
from psycopg2 import errors

# type names that refer to Windmill Resources
postgresql = dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def conninfo(db: postgresql):
    """Convert a `postgresql` Windmill Resources to psycopg-style connection string"""
    return "dbname={dbname} user={user} password={password} host={host} port={port}".format(**db)


def main(
    kobo_api_key: str,
    form_id: str,
    db: postgresql,
    db_table_name: str,
    attachment_root: str = "/frizzle-persistent-storage/datalake",
    kobo_server_base_url: str = "https://kf.kobotoolbox.org",
):
    forms = fetch_kobo_forms(kobo_server_base_url, kobo_api_key, [form_id])
    logger.info(f"Found {len(forms)} matching form(s)")

    form_data = process_forms(kobo_server_base_url, kobo_api_key, forms, attachment_root)
    logger.info(f"Downloaded {len(form_data)} submissions, including attachments")

    db_writer = KoboDBWriter(conninfo(db), db_table_name)
    db_writer.handle_output(form_data)
    logger.info(f"Wrote response content to database table [{db_table_name}]")


def fetch_kobo_forms(server_base_url, kobo_api_key, form_ids):
    my_forms = []
    url = f"{server_base_url}/api/v2/assets.json"
    headers = {"Authorization": f"Token {kobo_api_key}"}
    payload = {}
    response = requests.request("GET", url, headers=headers, data=payload)
    response.raise_for_status()
    results = response.json().get("results", [])
    if not results:
        raise ValueError(f"No forms were fetched: {response.json()}")
    my_forms = {
        "dataset_id": next(
            (sanitize_form_name(res["name"]) for res in results if res.get("uid") in form_ids),
            None,
        ),
        "dataset_name": next((res["name"] for res in results if res.get("uid") in form_ids), None),
        "forms": [res for res in results if res.get("uid") in form_ids],
    }
    if len(my_forms["forms"]) != len(form_ids):
        raise ValueError(
            f"Did not find some form(s). It may be on a different server or accessible with a different API key: {[res.get('uid') for res in results]} != {form_ids}"
        )

    return my_forms


def process_forms(server_base_url, kobo_api_key, my_forms, attachment_root):
    result = {}
    form_data = []
    for index, form in enumerate(my_forms["forms"]):
        form_id = form.get("uid")
        # Download the form questions & metadata
        url = f"{server_base_url}/api/v2/assets/{form_id}/data/"
        headers = {
            "Authorization": f"Token {kobo_api_key}",
            "Accept": "application/json, text/javascript, */*; q=0.01",
        }

        response = requests.get(url, headers=headers)
        current_form_data = response.json()["results"]
        # incorporate the dataset_name/id for each submission to be inserted in the same way as other fields later on
        for submission in current_form_data:
            submission["dataset_name"] = my_forms["dataset_name"]
            submission["dataset_id"] = my_forms["dataset_id"]
        form_data.append(current_form_data)
        # TODO: store this in sql
        data = response.json()

        for result in data["results"]:
            # Check if the result has attachments
            if "_attachments" in result:
                for attachment in result["_attachments"]:
                    if "download_url" in attachment:
                        response = requests.get(attachment["download_url"], headers=headers)
                        if response.status_code == 200:
                            file_name = attachment["filename"]
                            # ie. datalake/{dataset_id}/attachments/{files}
                            save_path = os.path.join(
                                attachment_root,
                                my_forms["dataset_id"],
                                "attachments",
                                os.path.basename(file_name),
                            )
                            os.makedirs(os.path.dirname(save_path), exist_ok=True)
                            with open(save_path, "wb") as file:
                                file.write(response.content)
                            logger.debug("Download completed: " + attachment["download_url"])
                        else:
                            # TODO: add retries
                            logger.error("Failed downloading attachments.")
        submissions = current_form_data
        logger.info(
            f"Form {index + 1} (ID: {form_id}): Fetched {len(submissions)} submission(s)."
        )
    return form_data


def sanitize_form_name(form_name):
    # sanitizes form_name string for path creation
    name = re.sub(r"[\s()]", "_", form_name)
    name = re.sub(r"[^a-zA-Z0-9_-]", "", name)
    name = name.lstrip("-")
    return name if name else "default"


def _reverse_parts(k, sep="/"):
    return sep.join(reversed(k.split(sep)))


def _drop_nonsql_chars(s):
    return re.sub(r"[ ./?\[\]\\,<>(){}]", "", s)


def _shorten_and_uniqify(key, conflicts, maxlen):
    counter = 1
    new_key = key[:maxlen]
    while new_key in conflicts:
        new_key = "{}_{:03d}".format(key[: maxlen - 4], counter)
        counter += 1
    return new_key


def sanitize(
    message,
    column_renames,
    reverse_properties_separated_by=None,
    str_replace=[],
    maxlen=63,  # https://stackoverflow.com/a/27865772
):
    column_renames = column_renames.copy()
    sql_message = {}
    for original_key, value in message.items():
        if isinstance(value, list) or isinstance(value, dict):
            value = json.dumps(value)

        if original_key in column_renames:
            sql_message[column_renames[original_key]] = value
            continue

        # transform
        key = original_key
        if reverse_properties_separated_by:
            key = _reverse_parts(original_key, reverse_properties_separated_by)
        for args in str_replace:
            key = key.replace(*args)
        key = _drop_nonsql_chars(key)
        key = _shorten_and_uniqify(key, column_renames.values(), maxlen)

        column_renames[original_key] = key
        sql_message[key] = value
    return sql_message, column_renames


class KoboDBWriter:
    """
    Converts unstructured kobo forms data to structured SQL tables.

    Methods:
        __init__(db_connection_string, table_name): Initializes the KoboIOManager with the provided connection string and form response table to be used.

    """

    def __init__(self, db_connection_string, table_name, reverse_properties_separated_by="/"):
        """
        Component for syncing messages to a SQL database table. This component
        automatically handles table width resizing, row-level updates, most data type
        conversions, and sanitizing kobo field names to be SQL-compatible.

        Example of using `reverse_properties_separated_by`:
        - incoming field => `{"group1/group2/question": "How do you do?"}`
        - `reverse_properties_separated_by="/"`
        - SQL column/value => `{"question/group2/group1": "How do you do?"}`

        Parameters
        ----------
        db_connection_string : str
            The connection string required to establish a connection to the PostgreSQL database configured via definitions.
        table_name : str
            The response table that the form data will be stored configured via definitions.
        reverse_properties_separated_by : str
            An optional transformation of flat property names: split the name on the
            `reverse_properties_separated_by` character, reverse the parts, then re-concatenate
            them with the same separator.
        """
        self.db_connection_string = db_connection_string
        self.table_name = table_name
        self.reverse_separator = reverse_properties_separated_by

    def _get_conn(self):
        """
        Establishes a connection to the PostgreSQL database using the class's configured connection string.
        """
        return psycopg2.connect(dsn=self.db_connection_string)

    def _inspect_schema(self, table_name):
        """Fetches the column names of the given table."""
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"
        )
        columns = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return columns

    def _get_existing_cols(self, table_name):
        """Fetches the column names of the given table."""
        conn = self._get_conn()
        cursor = conn.cursor()
        if not self._table_exists(cursor, f"{table_name}__columns"):
            query = f"""
            CREATE TABLE public.{table_name}__columns (
            original_column VARCHAR(128) NULL,
            sql_column VARCHAR(64) NOT NULL);
            """
            cursor.execute(query)
            conn.commit()
            logger.info(f"Table {table_name}__columns created.")
        cursor.execute(
            f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"
        )
        columns = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return columns

    def _get_existing_mappings(self, table_name):
        """Fetches the current column names of the given form table."""
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute(f"SELECT original_column, sql_column FROM {table_name};")

        columns_dict = {row[0]: row[1] for row in cursor.fetchall()}
        cursor.close()
        conn.close()
        return columns_dict

    def _create_missing_mappings(self, table_name, missing_columns):
        """Generates and executes SQL statements to add missing mappings to the table."""
        try:
            with self._get_conn() as conn, conn.cursor() as cursor:
                for original_column, sql_column in missing_columns.items():
                    try:
                        query = f"""
                        INSERT INTO {table_name}__columns (original_column, sql_column)
                        VALUES ('{original_column}', '{sql_column}');
                        """
                        cursor.execute(query)
                    except errors.UniqueViolation:
                        logger.info(
                            f"Skipping insert of mappings into {table_name}__columns due to UniqueViolation, this mapping column has been accounted for already in the past: {sql_column}"
                        )
                        continue
                    except Exception as e:
                        logger.error(
                            f"An error occurred while creating missing columns {original_column},{sql_column} for {table_name}__columns: {e}"
                        )
                        raise
        finally:
            conn.close()

    def _table_exists(self, cursor, table_name):
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
        """Generates and executes SQL statements to add missing fields to the table."""
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
        Processes alerts metadata data from Dagster assets and inserts it into a PostgreSQL database. It iterates over each alerts metadata object, extracts relevant features and properties, and constructs SQL queries to insert these data into the database. After processing all features, it commits the transaction and closes the database connection.
        """
        db_connection_string = self.db_connection_string
        table_name = self.table_name

        conn = self._get_conn()
        cursor = conn.cursor()

        existing_fields = self._get_existing_cols(table_name)
        existing_columns_map = self._get_existing_mappings(table_name + "__columns")
        incoming_columns_dict = {}

        rows = []
        # There will always be only 1 single form to be processed
        form = outputs[0]
        # Iterate over each submission to collect the full set of columns needed
        original_to_sql = {}
        for submission in form:
            sanitized_columns_dict, updated_columns_map = sanitize(
                submission,
                existing_columns_map,
                reverse_properties_separated_by=self.reverse_separator,
            )
            rows.append((sanitized_columns_dict, existing_columns_map))
            original_to_sql.update(updated_columns_map)

        missing_map_keys = set()
        missing_field_keys = set()
        for sanitized, existing_mappings in rows:
            # what's in sanitized incoming that we are not supporting currently
            missing_map_keys.update(
                set(sanitized.keys()).difference(set(existing_mappings.values()))
            )
            diff = set(existing_mappings.values()).difference(existing_fields)

            # what's in existing mapping but it doesn't exist in the form table
            # NOTE: this can happen when the database is new based off legacy mapping
            missing_field_keys.update(set(existing_mappings.values()).difference(existing_fields))

            # what's in incoming keys but it doesn't exist in the form table
            missing_field_keys.update(set(sanitized.keys()).difference(existing_fields))

        missing_mappings = {}
        for m in missing_map_keys:
            # FIXME: need to clean from the database probably instead
            if m in ["g__coordinates", "_topic", "p___sender", "g__type"]:
                continue
            # TODO: write a test for this when it's empty
            # m is a missing key
            original = [key for key, val in original_to_sql.items() if val == m]
            if original:
                original = original[0]
            else:
                # we don't need to support this sql column
                # as it doesn't have a matched original to be mapped
                # and can be skipped
                continue
            sql = m
            missing_mappings[str(original)] = sql

        logger.info(f"New incoming map keys missing from db: {len(missing_mappings)}")

        if missing_mappings:
            self._create_missing_mappings(table_name, missing_mappings)
            time.sleep(10)

        logger.info(f"New incoming field keys missing from db: {len(missing_field_keys)}")

        if missing_field_keys:
            self._create_missing_fields(table_name, missing_field_keys)
            updated_fields = self._get_existing_cols(table_name)
            # assert updated_fields != existing_fields, "{table_name} fields have not been updated properly."

        logger.info(f"Inserting {len(rows)} submissions into DB.")

        for row, _ in rows:
            try:
                # Wrap column names in single quotes
                columns = ", ".join(f'"{name}"' for name in row.keys())

                # Prepare the values list, converting None to 'NULL'
                values = [f"{value}" if value is not None else None for value in row.values()]

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
