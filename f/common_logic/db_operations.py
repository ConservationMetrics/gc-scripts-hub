"""
This module provides functions and classes for interacting with PostgreSQL databases.
"""

import json
import logging
import time

from psycopg import Error, connect, errors, sql

from f.common_logic.identifier_utils import (
    normalize_identifier,
    sanitize_sql_message,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


postgresql = dict


def _normalize_value_for_comparison(value):
    """
    Normalize a value for comparison between new data and existing database rows.

    Treats empty strings as None (NULL) to match how StructuredDBWriter stores values.
    Converts all non-None, non-empty values to strings for consistent comparison.

    Parameters
    ----------
    value : any
        The value to normalize.

    Returns
    -------
    str or None
        The normalized value as a string, or None if the value is None or empty string.
    """
    if value is None or value == "":
        return None
    return str(value)


def conninfo(db: postgresql):
    """Convert a `postgresql` Windmill Resources to psycopg3-style connection string"""
    # password is optional
    password_part = f" password={db['password']}" if "password" in db else ""
    conn = "dbname={dbname} user={user} host={host} port={port}".format(**db)
    return conn + password_part


def check_if_table_exists(
    db_connection_string: str, table_name: str, schema: str = "public"
):
    """Check if a table exists in the database, returns a boolean. Default schema is public."""
    with connect(db_connection_string, autocommit=True) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = %s
                )
                """,
                (schema, table_name),
            )
            return cursor.fetchone()[0]


def fetch_tables_from_postgres(db_connection_string: str):
    """Fetch all table names from the public schema of the PostgreSQL database. Returns a list of table names."""
    try:
        with connect(db_connection_string, autocommit=True) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT table_name FROM information_schema.tables
                    WHERE table_schema = 'public'
                """)
                return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"Error fetching tables: {e}")
        return []


def fetch_data_from_postgres(db_connection_string: str, table_name: str):
    """
    Fetches all data from a specified PostgreSQL table.

    Parameters
    ----------
        db_connection_string (str): The connection string for the PostgreSQL database.
        table_name (str): The name of the table to fetch data from.

    Returns
    -------
        tuple: A tuple containing a list of column names and a list of rows fetched from the table.
    """

    try:
        with connect(db_connection_string, autocommit=True) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    sql.SQL("SELECT * FROM {table_name}").format(
                        table_name=sql.Identifier(table_name)
                    )
                )
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
    except Error as e:
        logger.error(f"Error fetching data from {table_name}: {e}")
        raise

    logger.info(f"Data fetched from {table_name}")
    return columns, rows


def summarize_new_rows_updates_and_columns(
    db: postgresql,
    table_name: str,
    new_data: list[dict],
    primary_key: str = "_id",
    str_replace: list[tuple[str, str]] = None,
    reverse_properties_separated_by: str = None,
):
    """
    Compare uploaded dataset against existing table to determine impact.

    IMPORTANT: This function must stay in sync with StructuredDBWriter behavior.
    When calling this function, pass the same str_replace and reverse_properties_separated_by
    parameters that you intend to use when creating the StructuredDBWriter instance.

    Value comparison uses _normalize_value_for_comparison() to match how StructuredDBWriter
    stores values (empty strings treated as NULL).

    Parameters
    ----------
    db : postgresql
        Database connection dictionary.
    table_name : str
        Name of the existing table to compare against.
    new_data : list[dict]
        List of dictionaries representing the new data rows to be imported.
    primary_key : str, optional
        Column name to use as primary key for identifying updates (default: "_id").
    str_replace : list of tuple, optional
        List of (old, new) strings to apply during column name sanitization.
        Must match the StructuredDBWriter settings. Defaults to [("/", "__")].
    reverse_properties_separated_by : str or None, optional
        If provided, splits keys on this character, reverses segments, and rejoins.
        Must match the StructuredDBWriter settings. Defaults to None.

    Returns
    -------
    tuple[int, int, int]
        A tuple containing:
        - new_rows : Number of rows that would be inserted (new primary keys)
        - updates : Number of existing rows that would be updated (changed values)
        - new_columns : Number of new columns that would be added to the table
    """
    # Apply same defaults as StructuredDBWriter to ensure consistency
    if str_replace is None:
        str_replace = [("/", "__")]
    if not new_data:
        return 0, 0, 0

    with connect(dsn=conninfo(db), autocommit=True) as conn:
        with conn.cursor() as cursor:
            # Get existing columns in the table
            cursor.execute(
                """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s AND table_schema = 'public'
                """,
                (table_name,),
            )
            existing_columns = {row[0] for row in cursor.fetchall()}
            logger.info(f"Existing table has {len(existing_columns)} columns")

            # Get all column names from new data
            # IMPORTANT: Preserve the order of columns as they appear in the first row
            # because sanitize_sql_message assigns collision suffixes based on iteration order
            new_data_columns_raw = []
            seen = set()
            for row in new_data:
                for col in row.keys():
                    if col not in seen:
                        new_data_columns_raw.append(col)
                        seen.add(col)
            logger.info(f"New data has {len(new_data_columns_raw)} columns (raw)")

            # Normalize new data column names using sanitize_sql_message to match
            # how StructuredDBWriter actually creates columns (with collision handling)
            # Create a dummy row with all columns to get the normalized column names
            # Use the first actual row to preserve the original key order
            dummy_row = new_data[0].copy() if new_data else {}
            # Add any missing columns from other rows (preserve order)
            for col in new_data_columns_raw:
                if col not in dummy_row:
                    dummy_row[col] = None

            sanitized, column_mapping = sanitize_sql_message(
                dummy_row,
                column_renames={},  # Start with empty mappings
                reverse_properties_separated_by=reverse_properties_separated_by,
                str_replace=str_replace,
                maxlen=63,
            )

            # Extract just the normalized column names (values from column_mapping)
            new_data_columns_normalized = set(column_mapping.values())

            logger.info(
                f"New data has {len(new_data_columns_normalized)} columns (normalized)"
            )

            # Calculate new columns using normalized names
            new_columns_set = new_data_columns_normalized - existing_columns
            new_columns = len(new_columns_set)

            if new_columns > 0:
                logger.info(f"Detected {new_columns} new columns")
            else:
                logger.info("No new columns detected")

            # Get existing primary keys and their data
            # Normalize primary key name to match database
            normalized_primary_key = normalize_identifier(primary_key)

            if normalized_primary_key in existing_columns:
                # Fetch only the columns that exist in both datasets for comparison
                common_columns = existing_columns & new_data_columns_normalized
                if not common_columns:
                    # No common columns - all rows are new
                    return len(new_data), 0, new_columns

                # Build query to fetch existing data
                column_list = sql.SQL(", ").join(map(sql.Identifier, common_columns))
                query = sql.SQL("SELECT {columns} FROM {table}").format(
                    columns=column_list, table=sql.Identifier(table_name)
                )
                cursor.execute(query)

                # Use the column_mapping we created earlier (from sanitize_sql_message)
                # This already has the correct raw -> normalized mappings with collision handling
                raw_to_normalized = column_mapping

                # Create reverse mapping (normalized to raw) for looking up values in new_data
                normalized_to_raw = {v: k for k, v in raw_to_normalized.items()}

                # Create lookup of existing rows by primary key
                existing_rows = {}
                for row in cursor.fetchall():
                    row_dict = dict(zip(common_columns, row))
                    if normalized_primary_key in row_dict:
                        # Normalize values for comparison using shared logic
                        normalized = {
                            k: _normalize_value_for_comparison(v)
                            for k, v in row_dict.items()
                        }
                        existing_rows[str(row_dict[normalized_primary_key])] = (
                            normalized
                        )

                # Compare new data against existing
                new_rows_count = 0
                updates_count = 0

                for new_row in new_data:
                    # Get primary key value using raw column name
                    pk_value = str(new_row.get(primary_key))

                    if pk_value not in existing_rows:
                        # This is a new row
                        new_rows_count += 1
                    else:
                        # Check if any values differ in common columns
                        existing_row = existing_rows[pk_value]
                        has_changes = False

                        for normalized_col in common_columns:
                            # Map normalized column back to raw column name to get value from new_data
                            raw_col = normalized_to_raw.get(
                                normalized_col, normalized_col
                            )
                            new_val = new_row.get(raw_col)
                            existing_val = existing_row.get(normalized_col)

                            # Normalize for comparison using shared logic
                            new_val_norm = _normalize_value_for_comparison(new_val)
                            existing_val_norm = _normalize_value_for_comparison(
                                existing_val
                            )

                            if new_val_norm != existing_val_norm:
                                has_changes = True
                                break

                        if has_changes:
                            updates_count += 1

                return new_rows_count, updates_count, new_columns

            else:
                # Primary key doesn't exist in table - all rows are new
                return len(new_data), 0, new_columns


class StructuredDBWriter:
    """
     StructuredDBWriter writes structured or semi-structured data (e.g., form submissions, GeoJSON features)
     into a PostgreSQL table. It optionally supports dynamic schema evolution and column mapping, or can rely on a predefined schema setup.

     Parameters
     ----------
     db_connection_string : str
         PostgreSQL connection string.
     table_name : str
         Destination table name for the data.
     suffix : bool
        If provided, appends `__<suffix>` to the table name. The full name will be truncated
        to 63 characters to satisfy PostgreSQL’s identifier length limit. This is useful for
        creating auxiliary tables (e.g., labels, columns) associated with a primary data table.
    use_mapping_table : bool
       If True, maintains a mapping table that maps original keys to SQL-safe column names.
    reverse_properties_separated_by : str or None
       If provided, splits keys on this character, reverses segments, and rejoins — useful for nested property flattening.
     str_replace : list of tuple, optional
         List of (old, new) strings to apply to keys during sanitization.
     predefined_schema : callable or None, optional
         If provided, this function is executed to create or validate the schema before inserting any data.
         Signature: `(cursor, table_name) -> None`

     Typical Use Cases
     -----------------
     - Writing cleaned KoboToolbox or ODK form data to a SQL table.
     - Ingesting GeoJSON features with flattened geometry and properties.
     - Storing alert data with a strict, predefined schema.
    """

    def __init__(
        self,
        db_connection_string,
        table_name,
        suffix=None,
        use_mapping_table=False,
        reverse_properties_separated_by=None,
        str_replace=[("/", "__")],
        predefined_schema=None,
    ):
        self.db_connection_string = db_connection_string

        # Table name is converted to lowercase to ensure consistency
        table_name = table_name.lower()
        self.base_table_name = table_name

        # Safely truncate the table to 63 characters
        # If suffix is provided (e.g., "labels"), create a derived table name.
        # TODO: ...while retaining uniqueness
        self.suffix = suffix

        if suffix:
            max_base_length = 63 - len(f"__{suffix}")
            base = table_name[:max_base_length]
            self.table_name = f"{base}__{suffix}"
        else:
            self.table_name = table_name[:63]
        self.use_mapping_table = use_mapping_table
        self.reverse_separator = reverse_properties_separated_by
        self.str_replace = str_replace
        self.predefined_schema = predefined_schema

    def _get_conn(self):
        """
        Establishes a connection to the PostgreSQL database using the class's configured connection string.
        """
        return connect(self.db_connection_string, autocommit=True)

    def _inspect_schema(self, table_name):
        """Fetches the column names of the given table."""
        with self._get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
                    (table_name,),
                )
                return [row[0] for row in cursor.fetchall()]

    def _get_existing_mappings(self, table_name):
        """Fetches the current column names of the given form table."""
        with self._get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT original_column, sql_column FROM {table_name};")
                return {row[0]: row[1] for row in cursor.fetchall()}

    def _create_missing_mappings(self, table_name, missing_columns):
        """Generates and executes SQL statements to add missing mappings to the table.

        Each column mapping is created in its own transaction;
        if this fails for some column (e.g. already exists or other error), that will not affect the other columns' mappings.
        """
        with self._get_conn() as conn:
            with conn.cursor() as cursor:
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

    def _get_existing_cols(self, table_name, columns_table_name):
        """Fetches the column names of the given table."""
        with self._get_conn() as conn:
            with conn.cursor() as cursor:
                query = sql.SQL("""
                CREATE TABLE IF NOT EXISTS {columns_table_name} (
                original_column VARCHAR(128) NULL,
                sql_column VARCHAR(64) NOT NULL);
                """).format(columns_table_name=sql.Identifier(columns_table_name))
                cursor.execute(query)
                cursor.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
                    (table_name,),
                )
                return [row[0] for row in cursor.fetchall()]

    def _create_missing_fields(self, table_name, missing_columns):
        """Generates and executes SQL statements to add missing fields to the table.

        Each column is created in its own transaction;
        if this fails for some column (e.g. already exists or other error), that will not affect the other columns being created.
        """
        table_name = sql.Identifier(table_name)
        with self._get_conn() as conn:
            with conn.cursor() as cursor:
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
        cursor : psycopg cursor
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

    def handle_output(self, submissions):
        table_name = self.table_name

        if self.use_mapping_table:
            columns_table_name = f"{table_name[:54]}__columns"
            existing_fields = self._get_existing_cols(table_name, columns_table_name)
            existing_mappings = self._get_existing_mappings(columns_table_name)
        else:
            existing_fields = self._inspect_schema(table_name)
            existing_mappings = {}

        rows = []
        original_to_sql = {}

        for submission in submissions:
            sanitized, updated = sanitize_sql_message(
                submission,
                existing_mappings,
                reverse_properties_separated_by=self.reverse_separator,
                str_replace=self.str_replace,
            )
            rows.append((sanitized, existing_mappings))
            original_to_sql.update(updated)

        missing_map_keys = set()
        missing_field_keys = set()

        for sanitized, mappings in rows:
            # Identify keys in the sanitized data that are not currently supported by existing mappings
            colnames = mappings.values() if self.use_mapping_table else sanitized.keys()
            missing_map_keys.update(set(sanitized.keys()) - set(mappings.values()))
            # Identify keys in existing mappings that do not exist in the database table
            # NOTE: This can occur when the database is newly created based on legacy mappings
            missing_field_keys.update(set(colnames) - set(existing_fields))
            # Identify keys in the sanitized data that do not exist in the database table
            missing_field_keys.update(set(sanitized.keys()) - set(existing_fields))

        if self.use_mapping_table and missing_map_keys:
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

            logger.info(
                f"New incoming map keys missing from db: {len(missing_mappings)}"
            )

            self._create_missing_mappings(columns_table_name, missing_mappings)
            time.sleep(10)

        inserted_count = 0
        updated_count = 0

        with self._get_conn() as conn, conn.cursor() as cursor:
            # Use predefined schema if provided, else mutate schema dynamically
            if self.predefined_schema:
                self.predefined_schema(cursor, table_name)
            elif missing_field_keys:
                logger.info(
                    f"New incoming field keys missing from db: {len(missing_field_keys)}"
                )
                self._create_missing_fields(table_name, missing_field_keys)

            logger.info(f"Attempting to write {len(rows)} submissions to the DB.")

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

            logger.info(f"Total rows inserted: {inserted_count}")
            logger.info(f"Total rows updated: {updated_count}")

        # Return True if there were new inserts
        return inserted_count > 0
