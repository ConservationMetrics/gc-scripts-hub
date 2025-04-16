# requirements:
# psycopg2-binary

import json
import logging
from pathlib import Path

from psycopg2 import connect, errors, sql

from f.common_logic.db_operations import conninfo, postgresql

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(
    db: postgresql,
    db_table_name: str,
    geojson_path: str,
    attachment_root: str = "/persistent-storage/datalake/",
    delete_geojson_file: bool = False,
):
    geojson_path = Path(attachment_root) / Path(geojson_path)
    transformed_geojson_data = transform_geojson_data(geojson_path)

    db_writer = GeoJSONDbWriter(conninfo(db), db_table_name)
    db_writer.handle_output(transformed_geojson_data)

    if delete_geojson_file:
        delete_geojson_file(geojson_path)


def transform_geojson_data(geojson_path):
    """
    Transforms GeoJSON data from a file into a list of dictionaries suitable for database insertion.

    Args:
        geojson_path (str or Path): The file path to the GeoJSON file.

    Returns:
        list: A list of dictionaries where each dictionary represents a GeoJSON feature with keys:
              '_id' for the feature's unique identifier,
              'g__type' for the geometry type,
              'g__coordinates' for the geometry coordinates,
              and any additional properties from the feature.
    """
    with open(geojson_path, "r") as f:
        geojson_data = json.load(f)

    transformed_geojson_data = []
    for feature in geojson_data["features"]:
        transformed_feature = {
            "_id": feature[
                "id"
            ],  # Assuming that the GeoJSON feature has unique "id" field that can be used as the primary key
            "g__type": feature["geometry"]["type"],
            "g__coordinates": feature["geometry"]["coordinates"],
            **feature.get("properties", {}),
        }
        transformed_geojson_data.append(transformed_feature)
    return transformed_geojson_data


class GeoJSONDbWriter:
    """
    Converts GeoJSON spatial data to structured SQL tables.
    """

    def __init__(self, db_connection_string, table_name):
        """
        Initializes the GeoJSONIOManager with the provided connection string and form response table to be used.
        """
        self.db_connection_string = db_connection_string
        self.table_name = table_name

    def _get_conn(self):
        """
        Establishes a connection to the PostgreSQL database using the class's configured connection string.
        """
        return connect(dsn=self.db_connection_string)

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

    def handle_output(self, outputs):
        """
        Inserts GeojSON spatial data into the specified PostgreSQL database table.
        It checks the database schema and adds any missing fields, then constructs
        and executes SQL insert queries to store the data. After processing all data,
        it commits the transaction and closes the database connection.
        """
        table_name = self.table_name

        conn = self._get_conn()
        cursor = conn.cursor()

        existing_fields = self._inspect_schema(table_name)
        rows = []
        for entry in outputs:
            sanitized_entry = {k: v for k, v in entry.items()}
            rows.append(sanitized_entry)

        missing_field_keys = set()
        for row in rows:
            missing_field_keys.update(set(row.keys()).difference(existing_fields))

        if missing_field_keys:
            self._create_missing_fields(table_name, missing_field_keys)

        logger.info(f"Attempting to write {len(rows)} submissions to the DB.")

        inserted_count = 0
        updated_count = 0

        for row in rows:
            try:
                cols, vals = zip(*row.items())

                # Serialize lists, dict values to JSON text
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


def delete_geojson_file(
    geojson_path: str,
):
    """
    Deletes the GeoJSON file after processing.

    Parameters
    ----------
    geojson_path : str
        The path to the GeoJSON file to delete.
    """
    try:
        geojson_path.unlink()
        logger.info(f"Deleted GeoJSON file: {geojson_path}")
    except FileNotFoundError:
        logger.warning(f"GeoJSON file not found: {geojson_path}")
    except Exception as e:
        logger.error(f"Error deleting GeoJSON file: {e}")
        raise
