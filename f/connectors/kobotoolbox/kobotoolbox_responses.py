# requirements:
# psycopg2-binary
# requests~=2.32

import json
import logging
import re
import time
from pathlib import Path

import psycopg2
import requests
from psycopg2 import errors, sql

# type names that refer to Windmill Resources
postgresql = dict
c_kobotoolbox_account = dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def conninfo(db: postgresql):
    """Convert a `postgresql` Windmill Resources to psycopg-style connection string"""
    # password is optional
    password_part = f" password={db['password']}" if "password" in db else ""
    conn = "dbname={dbname} user={user} host={host} port={port}".format(**db)
    return conn + password_part


def main(
    kobotoolbox: c_kobotoolbox_account,
    form_id: str,
    db: postgresql,
    db_table_name: str,
    attachment_root: str = "/frizzle-persistent-storage/datalake",
):
    kobo_server_base_url = kobotoolbox["server_url"]
    kobo_api_key = kobotoolbox["api_key"]

    form_data = download_form_responses_and_attachments(
        kobo_server_base_url, kobo_api_key, form_id, db_table_name, attachment_root
    )

    transformed_form_data = format_geometry_fields(form_data)

    db_writer = KoboDBWriter(conninfo(db), db_table_name)
    db_writer.handle_output(transformed_form_data)
    logger.info(
        f"KoboToolbox responses successfully written to database table: [{db_table_name}]"
    )


def _download_submission_attachments(
    submission, db_table_name, attachment_root, headers
):
    """Download and save attachments from a form submission.

    Parameters
    ----------
    submission : dict
        The form submission data
    dataset_id : str
        The identifier for the dataset, used to organize the save path.
    attachment_root : str
        The base directory where attachments will be stored.
    headers : dict
        HTTP headers required for downloading the attachments.

    Returns
    -------
    None
    """

    for attachment in submission["_attachments"]:
        if "download_url" in attachment:
            response = requests.get(attachment["download_url"], headers=headers)
            if response.status_code == 200:
                file_name = attachment["filename"]
                save_path = (
                    Path(attachment_root)
                    / db_table_name
                    / "attachments"
                    / Path(file_name).name
                )
                save_path.parent.mkdir(parents=True, exist_ok=True)
                with open(save_path, "wb") as file:
                    file.write(response.content)
                logger.debug(f"Download completed: {attachment['download_url']}")
            else:
                logger.error(
                    f"Failed downloading attachment: {attachment['download_url']}"
                )


def download_form_responses_and_attachments(
    server_base_url, kobo_api_key, form_id, db_table_name, attachment_root
):
    """Download form responses and their attachments from the KoboToolbox API.

    Parameters
    ----------
    server_base_url : str
        The base URL of the KoboToolbox server.
    kobo_api_key : str
        The API key for authenticating requests to the KoboToolbox server.
    form_id : str
        The unique identifier of the form to download.
    attachment_root : str
        The root directory where attachments will be saved.

    Returns
    -------
    list
        A list of form submissions data.
    """
    headers = {
        "Authorization": f"Token {kobo_api_key}",
        "Accept": "application/json, text/javascript, */*; q=0.01",
    }
    # First get the name of the form. You have to hit a different endpoint just for this.
    form_uri = f"{server_base_url}/api/v2/assets/{form_id}/"
    response = requests.get(form_uri, headers=headers)
    response.raise_for_status()
    data_uri = response.json()["data"]
    form_name = response.json().get("name")

    # Next download the form questions & metadata
    # FIXME: need to paginate. Maximum results per page is 30000.
    response = requests.get(data_uri, headers=headers)
    response.raise_for_status()

    form_submissions = response.json()["results"]

    for submission in form_submissions:
        submission["dataset_name"] = form_name

        # Download attachments for each submission, if they exist
        if "_attachments" in submission:
            _download_submission_attachments(
                submission, db_table_name, attachment_root, headers
            )

    logger.info(
        f"[Form {form_id}] Downloaded {len(form_submissions)} submission(s), including attachments."
    )
    return form_submissions


def format_geometry_fields(form_data):
    """Transform KoboToolbox form data by formatting geometry fields for SQL database insertion.

    Parameters
    ----------
    form_data : list
        A list of form submissions downloaded from the KoboToolbox API.

    Returns
    -------
    list
        A list of transformed form submissions.
    """
    for submission in form_data:
        geolocation = submission.get("_geolocation")
        if geolocation:
            submission["g__type"] = "Point"
            submission["g__coordinates"] = geolocation
            del submission["_geolocation"]

    return form_data


def _reverse_parts(k, sep="/"):
    """Reverse the parts of a string separated by a given separator.

    Parameters
    ----------
    k : str
        The string to be reversed.
    sep : str, optional
        The separator used to split and join the string parts, by default "/".

    Returns
    -------
    str
        The string with its parts reversed.
    """
    return sep.join(reversed(k.split(sep)))


def _drop_nonsql_chars(s):
    """Remove non-SQL compatible characters from a string.

    Parameters
    ----------
    s : str
        The string from which to remove non-SQL characters.

    Returns
    -------
    str
        The cleaned string with non-SQL characters removed.
    """
    return re.sub(r"[ ./?\[\]\\,<>(){}]", "", s)


def shorten_and_uniqify(identifier, conflicts, maxlen):
    """Shorten an identifier and ensure its uniqueness within a set of conflicts.

    This function truncates an identifier to a specified maximum length and appends a
    numeric suffix if necessary to ensure uniqueness within a set of conflicting identifiers.

    Parameters
    ----------
    identifier : str
        The original identifier to be shortened and made unique.
    conflicts : set
        A set of identifiers that the new identifier must not conflict with.
    maxlen : int
        The maximum allowed length for the identifier.

    Returns
    -------
    str
        A shortened and unique version of the identifier.
    """
    counter = 1
    new_identifier = identifier[:maxlen]
    while new_identifier in conflicts:
        new_identifier = "{}_{:03d}".format(identifier[: maxlen - 4], counter)
        counter += 1
    return new_identifier


def sanitize(
    message,
    column_renames,
    reverse_properties_separated_by=None,
    str_replace=[],
    maxlen=63,  # https://stackoverflow.com/a/27865772
):
    """Sanitize a message for SQL compatibility and rename columns.

    This function processes a message dictionary, converting lists and dictionaries
    to JSON strings, renaming columns based on provided mappings, and ensuring
    SQL compatibility of keys.

    Parameters
    ----------
    message : dict
        The original message dictionary to be sanitized.
    column_renames : dict
        A dictionary mapping original column names to their new names.
    reverse_properties_separated_by : str, optional
        A separator for reversing property names, by default None.
    str_replace : list, optional
        A list of tuples specifying string replacements, by default [].
    maxlen : int, optional
        The maximum length for SQL-compatible keys, by default 63.

    Returns
    -------
    sanitized_sql_message : dict
        The sanitized message dictionary with SQL-compatible keys.
    updated_column_renames : dict
        The updated column renames dictionary.
    """

    updated_column_renames = column_renames.copy()
    sanitized_sql_message = {}
    for original_key, value in message.items():
        if isinstance(value, list) or isinstance(value, dict):
            value = json.dumps(value)

        if original_key in updated_column_renames:
            sanitized_sql_message[updated_column_renames[original_key]] = value
            continue

        key = original_key
        if reverse_properties_separated_by:
            key = _reverse_parts(original_key, reverse_properties_separated_by)
        for args in str_replace:
            key = key.replace(*args)
        key = _drop_nonsql_chars(key)
        key = shorten_and_uniqify(key, updated_column_renames.values(), maxlen)

        updated_column_renames[original_key] = key
        sanitized_sql_message[key] = value
    return sanitized_sql_message, updated_column_renames


class KoboDBWriter:
    """
    Converts unstructured kobo forms data to structured SQL tables.

    Methods:
        __init__(db_connection_string, table_name): Initializes the KoboIOManager with the provided connection string and form response table to be used.

    """

    def __init__(
        self, db_connection_string, table_name, reverse_properties_separated_by="/"
    ):
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
            "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
            (table_name,),
        )
        columns = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return columns

    def _get_existing_cols(self, table_name, columns_table_name):
        """Fetches the column names of the given table."""
        conn = self._get_conn()
        cursor = conn.cursor()

        query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {columns_table_name} (
        original_column VARCHAR(128) NULL,
        sql_column VARCHAR(64) NOT NULL);
        """).format(columns_table_name=sql.Identifier(columns_table_name))
        cursor.execute(query)
        conn.commit()
        cursor.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
            (table_name,),
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
                        INSERT INTO {table_name} (original_column, sql_column)
                        VALUES ('{original_column}', '{sql_column}');
                        """
                        cursor.execute(query)
                    except errors.UniqueViolation:
                        logger.info(
                            f"Skipping insert of mappings into {table_name} due to UniqueViolation, this mapping column has been accounted for already in the past: {sql_column}"
                        )
                        continue
                    except Exception as e:
                        logger.error(
                            f"An error occurred while creating missing columns {original_column},{sql_column} for {table_name}: {e}"
                        )
                        raise
        finally:
            conn.close()

    def _create_missing_fields(self, table_name, missing_columns):
        """Generates and executes SQL statements to add missing fields to the table."""
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

    def handle_output(self, all_submissions):
        """
        Flattens kobo form submissions and inserts them into a PostgreSQL database.

        Parameters
        ----------
        all_submissions : list of dict
        """
        table_name = self.table_name
        columns_table_name = (
            f"{shorten_and_uniqify(f'{table_name}', set(), 54)}__columns"
        )

        conn = self._get_conn()
        cursor = conn.cursor()

        existing_fields = self._get_existing_cols(table_name, columns_table_name)
        existing_columns_map = self._get_existing_mappings(columns_table_name)

        rows = []
        # Iterate over each submission to collect the full set of columns needed
        original_to_sql = {}
        for submission in all_submissions:
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
            # Identify keys in the sanitized data that are not currently supported by existing mappings
            missing_map_keys.update(
                set(sanitized.keys()).difference(set(existing_mappings.values()))
            )
            # Identify keys in existing mappings that do not exist in the database table
            # NOTE: This can occur when the database is newly created based on legacy mappings
            missing_field_keys.update(
                set(existing_mappings.values()).difference(existing_fields)
            )

            # Identify keys in the sanitized data that do not exist in the database table
            missing_field_keys.update(set(sanitized.keys()).difference(existing_fields))

        missing_mappings = {}
        for m in missing_map_keys:
            # TODO: Write a test for this when it's empty
            original = [key for key, val in original_to_sql.items() if val == m]
            if original:
                original = original[0]
            else:
                # Skip this SQL column as it has no corresponding original key to map from
                continue
            sql = m
            missing_mappings[str(original)] = sql

        logger.info(f"New incoming map keys missing from db: {len(missing_mappings)}")

        if missing_mappings:
            self._create_missing_mappings(columns_table_name, missing_mappings)
            time.sleep(10)

        logger.info(
            f"New incoming field keys missing from db: {len(missing_field_keys)}"
        )

        if missing_field_keys:
            self._create_missing_fields(table_name, missing_field_keys)

        logger.info(f"Inserting {len(rows)} submissions into DB.")

        for row, _ in rows:
            try:
                cols, vals = zip(*row.items())

                # Serialize lists and dict values to JSON text
                vals = list(vals)
                for i in range(len(vals)):
                    value = vals[i]
                    if isinstance(value, list) or isinstance(value, dict):
                        vals[i] = json.dumps(value)

                self._safe_insert(cursor, table_name, cols, vals)

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
