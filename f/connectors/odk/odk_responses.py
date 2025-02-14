# requirements:
# psycopg2-binary
# requests
# pyodk~=0.3.0

import json
import logging
import tempfile
import time
from pathlib import Path

import psycopg2
from psycopg2 import errors, sql
from pyodk.client import Client

from f.common_logic.db_connection import conninfo, postgresql
from f.common_logic.db_transformations import sanitize

# type names that refer to Windmill Resources
c_odk_config = dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_temp_config(odk_config: c_odk_config) -> Path:
    """Create a temporary TOML configuration file for PyODK and return its path.

    This configuration file is used by PyODK to set up a Client for interacting
    with ODK Central. The file includes necessary connection details such as
    base URL, username, password, and default project ID.

    The structure of the TOML file is as follows:

    [central]
    base_url = "<ODK Central Base URL>"
    username = "<ODK Central Username>"
    password = "<ODK Central Password>"
    default_project_id = <ODK Central Default Project ID>

    Returns
    -------
    Path
        The file path to the temporary TOML configuration file.
    """
    temp_file = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".toml")
    temp_file.write(f"""
[central]
base_url = "{odk_config["base_url"]}"
username = "{odk_config["username"]}"
password = "{odk_config["password"]}"
default_project_id = {odk_config["default_project_id"]}
""")
    temp_file.close()
    return Path(temp_file.name)


def main(
    odk_config: c_odk_config,
    form_id: str,
    db: postgresql,
    db_table_name: str,
    attachment_root: str = "/persistent-storage/datalake",
):
    config_path = get_temp_config(odk_config)

    try:
        project_id = odk_config["default_project_id"]
        client = Client(config_path=str(config_path))

        form_data = download_form_responses_and_attachments(
            client, project_id, form_id, db_table_name, attachment_root
        )

        transformed_form_data = format_geometry_fields(form_data)

        db_writer = ODKDBWriter(conninfo(db), db_table_name)
        db_writer.handle_output(transformed_form_data)
        logger.info(
            f"ODK responses successfully written to database table: [{db_table_name}]"
        )

    finally:
        config_path.unlink(missing_ok=True)


def _download_submission_attachments(
    client, project_id, form_id, submission, db_table_name, attachment_root
):
    """Download and save attachments from a form submission.

    Parameters
    ----------
    client : pyodk.client.Client
        The PyODK client object.
    project_id : int
        The unique identifier of the project where the form is located.
    form_id : str
        The unique identifier (xmlFormId) of the form to download
    submission : dict
        The form submission data
    db_table_name : str
        The name of the database table where the form submissions will be stored.
    attachment_root : str
        The base directory where attachments will be stored.

    Returns
    -------
    int
        The number of attachments skipped due to already existing on disk.

    Notes
    -----
    If the file already exists at the specified path, the function will skip downloading the file.
    """
    skipped_attachments = 0

    uuid = submission.get("_id")
    attachments_path = (
        f"projects/{project_id}/forms/{form_id}/submissions/{uuid}/attachments"
    )

    response = client.get(attachments_path)
    response.raise_for_status()

    attachments = response.json()

    for attachment in attachments:
        file_name = attachment["name"]
        save_path = (
            Path(attachment_root) / db_table_name / "attachments" / Path(file_name).name
        )
        if save_path.exists():
            logger.debug(f"File already exists, skipping download: {save_path}")
            skipped_attachments += 1
            continue

        file_path = f"{attachments_path}/{file_name}"
        response = client.get(file_path)

        if response.status_code == 200:
            save_path.parent.mkdir(parents=True, exist_ok=True)
            with open(save_path, "wb") as file:
                file.write(response.content)
            logger.debug(f"Download completed: {file_path}")
        else:
            logger.error(f"Failed downloading attachment: {file_path}")
    return skipped_attachments


def download_form_responses_and_attachments(
    client, project_id, form_id, db_table_name, attachment_root
):
    """Download form responses and their attachments from the ODK Central API.

    Parameters
    ----------
    client : pyodk.client.Client
        The PyODK client object.
    project_id : int
        The unique identifier of the project where the form is located.
    form_id : str
        The unique identifier (xmlFormId) of the form to download.
    db_table_name : str
        The name of the database table where the form submissions will be stored.
    attachment_root : str
        The root directory where attachments will be saved.

    Returns
    -------
    list
        A list of form submissions data.
    """

    form_submissions = client.submissions.get_table(form_id)["value"]

    skipped_attachments = 0

    for submission in form_submissions:
        submission["_id"] = submission.pop("__id")
        submission["dataset_name"] = form_id
        submission["data_source"] = "ODK"

        # Download attachments for each submission, if they exist
        if submission.get("__system", {}).get("attachmentsPresent", 0) != 0:
            skipped_attachments += _download_submission_attachments(
                client, project_id, form_id, submission, db_table_name, attachment_root
            )

    if skipped_attachments > 0:
        logger.info(f"Skipped downloading {skipped_attachments} media attachment(s).")

    logger.info(f"[Form {form_id}] Downloaded {len(form_submissions)} submission(s).")
    return form_submissions


def format_geometry_fields(form_data):
    """Transform ODK form data by formatting geometry fields for SQL database insertion.

    Note that ODK also stores altitude in the coordinates array, but we are only interested in extracting lat/long. But we preserve the location object in the transformed data in case it is needed.

    Parameters
    ----------
    form_data : list
        A list of form submissions downloaded from the ODK API.

    Returns
    -------
    list
        A list of transformed form submissions.
    """
    for submission in form_data:
        if "location" in submission and submission["location"]:
            location_data = submission["location"]

            if "coordinates" in location_data:
                coordinates = location_data["coordinates"]

                # Extract latitude and longitude only
                lon, lat = coordinates[:2]

                submission.update(
                    {
                        "g__type": location_data.get("type", "Point"),
                        "g__coordinates": [lon, lat],
                    }
                )

    return form_data


class ODKDBWriter:
    """
    Converts unstructured forms data to structured SQL tables.

    Methods:
        __init__(db_connection_string, table_name): Initializes the ODKDBWriter with the provided connection string and form response table to be used.

    """

    def __init__(
        self,
        db_connection_string,
        table_name,
        reverse_properties_separated_by="/",
        str_replace=[("/", "__")],
    ):
        """
        Component for syncing messages to a SQL database table. This component
        automatically handles table width resizing, row-level updates, most data type
        conversions, and sanitizing field names to be SQL-compatible.

        Example of using `reverse_properties_separated_by` and `str_replace`:
        - incoming field => `{"group1/group2/question": "How do you do?"}`
        - `reverse_properties_separated_by="/"`, `str_replace=[("/", "__")]`
        - SQL column/value => `{"question__group2__group1": "How do you do?"}`

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
        str_replace : list
            An optional list of tuples specifying string replacements to be applied to the
            property names.
        """
        self.db_connection_string = db_connection_string
        self.table_name = table_name
        self.reverse_separator = reverse_properties_separated_by
        self.str_replace = str_replace

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
        Executes a safe INSERT operation into a PostgreSQL table, ensuring data integrity and preventing SQL injection.
        This method also handles conflicts by updating existing records if necessary.

        The function first checks if a row with the same primary key (_id) already exists in the table. If it does,
        and the existing row's data matches the new values, the operation is skipped. Otherwise, it performs an
        INSERT operation. If a conflict on the primary key occurs, it updates the existing row with the new values.

        Parameters
        ----------
        cursor : psycopg2 cursor
            The database cursor used to execute SQL queries.
        table_name : str
            The name of the table where data will be inserted.
        columns : list of str
            The list of column names corresponding to the values being inserted.
        values : list
            The list of values to be inserted into the table, aligned with the columns.

        Returns
        -------
        tuple
            A tuple containing two integers: the count of rows inserted and the count of rows updated.
        """
        inserted_count = 0
        updated_count = 0

        # Check if there is an existing row that is different from the new values
        # We are doing this in order to keep track of which rows are actually updated
        # (Otherwise all existing rows would be added to updated_count)
        id_index = columns.index("_id")
        values[id_index] = str(values[id_index])
        select_query = sql.SQL("SELECT {fields} FROM {table} WHERE _id = %s").format(
            fields=sql.SQL(", ").join(map(sql.Identifier, columns)),
            table=sql.Identifier(table_name),
        )
        cursor.execute(select_query, (values[columns.index("_id")],))
        existing_row = cursor.fetchone()

        if existing_row and list(existing_row) == values:
            # No changes, skip the update
            return inserted_count, updated_count

        query = sql.SQL(
            "INSERT INTO {table} ({fields}) VALUES ({placeholders}) "
            "ON CONFLICT (_id) DO UPDATE SET {updates} "
            # The RETURNING clause is used to determine if the row was inserted or updated.
            # xmax is a system column in PostgreSQL that stores the transaction ID of the deleting transaction.
            # If xmax is 0, it means the row was newly inserted and not updated.
            "RETURNING (xmax = 0) AS inserted"
        ).format(
            table=sql.Identifier(table_name),
            fields=sql.SQL(", ").join(map(sql.Identifier, columns)),
            placeholders=sql.SQL(", ").join(sql.Placeholder() for _ in values),
            updates=sql.SQL(", ").join(
                sql.Composed(
                    [sql.Identifier(col), sql.SQL(" = EXCLUDED."), sql.Identifier(col)]
                )
                for col in columns
                if col != "_id"
            ),
        )

        cursor.execute(query, values)
        result = cursor.fetchone()
        if result and result[0]:
            inserted_count += 1
        else:
            updated_count += 1

        return inserted_count, updated_count

    def handle_output(self, all_submissions):
        """
        Flattens form submissions and inserts them into a PostgreSQL database.

        Parameters
        ----------
        all_submissions : list of dict
        """
        table_name = self.table_name
        columns_table_name = f"{table_name[:54]}__columns"

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
                str_replace=self.str_replace,
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

        logger.info(f"Attempting to write {len(rows)} submissions to the DB.")

        inserted_count = 0
        updated_count = 0

        for row, _ in rows:
            try:
                cols, vals = zip(*row.items())

                # Serialize lists and dict values to JSON text
                vals = list(vals)
                for i in range(len(vals)):
                    value = vals[i]
                    if isinstance(value, list) or isinstance(value, dict):
                        vals[i] = json.dumps(value)

                result_inserted_count, result_updated_count = self._safe_insert(
                    cursor, table_name, cols, vals
                )
                inserted_count += result_inserted_count
                updated_count += result_updated_count

            except Exception as e:
                logger.error(f"Error inserting data: {e}, {type(e).__name__}")
                conn.rollback()

            try:
                conn.commit()
            except Exception as e:
                logger.error(f"Error committing transaction: {e}")
                conn.rollback()

        logger.info(f"Total rows inserted: {inserted_count}")
        logger.info(f"Total rows updated: {updated_count}")

        cursor.close()
        conn.close()
