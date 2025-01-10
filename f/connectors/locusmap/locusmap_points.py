# requirements:
# psycopg2-binary
import csv
import json
import logging
import shutil
import uuid
from pathlib import Path

from psycopg2 import connect, errors, sql

# type names that refer to Windmill Resources
postgresql = dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def conninfo(db: postgresql):
    """Convert a `postgresql` Windmill Resources to psycopg-style connection string"""
    # password is optional
    password_part = f" password={db['password']}" if "password" in db else ""
    conn = "dbname={dbname} user={user} host={host} port={port}".format(**db)
    return conn + password_part


def main(
    db: postgresql,
    db_table_name: str,
    locusmap_tmp_path: str,
    attachment_root: str = "/frizzle-persistent-storage/datalake/",
):
    if Path(locusmap_tmp_path).suffix.lower() == ".zip":
        locusmap_points_tmp_path, locusmap_attachments_tmp_path = extract_locusmap_zip(
            locusmap_tmp_path
        )
    else:
        locusmap_points_tmp_path = Path(locusmap_tmp_path)
        if locusmap_points_tmp_path.suffix.lower() not in [".kml", ".gpx", ".csv"]:
            raise ValueError(
                "Unsupported file format. Only KML, GPX, and CSV are supported."
            )
        locusmap_attachments_tmp_path = None

    transformed_locusmap_points = transform_locusmap_points(locusmap_points_tmp_path)

    if locusmap_attachments_tmp_path:
        copy_locusmap_attachments(
            locusmap_attachments_tmp_path, db_table_name, attachment_root
        )

    db_writer = LocusMapDbWriter(conninfo(db), db_table_name)
    db_writer.handle_output(transformed_locusmap_points)

    delete_locusmap_tmp_files(
        locusmap_tmp_path, locusmap_points_tmp_path, locusmap_attachments_tmp_path
    )


def extract_locusmap_zip(locusmap_zip_tmp_path):
    """
    Extracts a Locus Map ZIP file containing point data and attachment files.

    Parameters
    ----------
    locusmap_zip_tmp_path : str
        The path to the temporary ZIP file containing Locus Map data.

    Returns
    -------
    tuple
        A tuple containing the paths to the extracted Points file and attachments directory.
    """
    locusmap_zip_tmp_path = Path(locusmap_zip_tmp_path)
    shutil.unpack_archive(locusmap_zip_tmp_path, locusmap_zip_tmp_path.parent)
    logger.info("Extracted Locus Map ZIP file.")

    locusmap_attachments_tmp_path = locusmap_zip_tmp_path.with_name(
        locusmap_zip_tmp_path.stem + "-attachments"
    )
    if not locusmap_attachments_tmp_path.exists():
        locusmap_attachments_tmp_path = None

    extracted_files = list(locusmap_zip_tmp_path.parent.glob("*.*"))
    for file in extracted_files:
        if file.suffix.lower() in [".kml", ".gpx", ".csv"]:
            locusmap_points_tmp_path = file
            break
    else:
        raise ValueError(
            "Unsupported file format. Only KML, GPX, and CSV are supported."
        )

    return locusmap_points_tmp_path, locusmap_attachments_tmp_path


def transform_locusmap_points(locusmap_points_tmp_path):
    """
    Transforms Locus Map point data from a CSV file into a list of dictionaries.

    TODO: Support GPX, KML.

    Parameters
    ----------
    locusmap_points_tmp_path : str
        The path to the temporary CSV file containing LocusMap point data.

    Returns
    -------
    list
        A list of dictionaries, where each dictionary represents a transformed LocusMap point.

    Notes
    -----
    The function reads the CSV file and performs the following transformations:
    - Converts the 'attachments' field from a string to a list of strings.
    - Creates 'g__coordinates' and 'g__type' fields from the 'lat' and 'lon' fields.
    - Generates a UUID for each point based on its dictionary contents and assigns it to the '_id' field.

    The transformed points are returned as a list of dictionaries.
    """
    transformed_points = []
    points_processed = 0

    with open(locusmap_points_tmp_path, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            points_processed += 1
            point = {
                # Transform the 'attachments' field to a string composed of comma-separated filenames
                k: (
                    ", ".join(
                        v.split("|")[-1].split("/")[-1] for v in row[k].split("|")
                    )
                    if k == "attachments"
                    else v
                )
                for k, v in row.items()
            }

            if "lat" in point and "lon" in point:
                lat, lon = point.pop("lat"), point.pop("lon")
                point["g__coordinates"] = f"[{lon}, {lat}]"
                point["g__type"] = "Point"

            # Generate a UUID based on the dictionary contents
            point_json = json.dumps(point, sort_keys=True)
            point["_id"] = str(uuid.uuid5(uuid.NAMESPACE_OID, point_json))

            transformed_points.append(point)

    logger.info(f"Processed {points_processed} points from LocusMap.")
    return transformed_points


def copy_locusmap_attachments(
    locusmap_attachments_tmp_path, db_table_name, attachment_root
):
    """
    Copies Locus Map attachment files from a temporary directory to a specified root directory.

    Parameters
    ----------
    locusmap_attachments_tmp_path : str
        The path to the temporary directory containing Locus Map attachment files.
    db_table_name : str
        The name of the database table where the point data will be stored.
    attachment_root : str
        The root directory where the attachment files will be copied.
    """
    shutil.copytree(
        locusmap_attachments_tmp_path, Path(attachment_root) / db_table_name
    )

    logger.info(f"Copied Locus Map attachments to {attachment_root}/{db_table_name}.")


class LocusMapDbWriter:
    """
    Converts unstructured Locus Map point data to structured SQL tables.
    """

    def __init__(self, db_connection_string, table_name):
        """
        Initializes the CoMapeoIOManager with the provided connection string and form response table to be used.
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
        Inserts Locus Map point data into the specified PostgreSQL database table.
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


def delete_locusmap_tmp_files(
    locusmap_tmp_path, locusmap_attachments_tmp_path=None, locusmap_points_tmp_path=None
):
    """
    Deletes the temporary files and attachments directory used for Locus Map point data.

    Parameters
    ----------
    locusmap_tmp_path : str or Path
        The path to the temporary file containing Locus Map data (CSV, ZIP, etc.).
    locusmap_points_tmp_path : str or Path, optional
        The path to the temporary CSV file extracted from the ZIP file, if applicable.
    locusmap_attachments_tmp_path : str or Path, optional
        The path to the temporary directory containing Locus Map attachment files.
    """
    from pathlib import Path

    paths = [
        Path(p)
        for p in (
            locusmap_tmp_path,
            locusmap_attachments_tmp_path,
            locusmap_points_tmp_path,
        )
        if p
    ]

    for path in paths:
        try:
            if path.is_dir():
                shutil.rmtree(path, ignore_errors=True)
            else:
                path.unlink(missing_ok=True)
        except Exception as e:
            logger.warning(f"Failed to delete {path}: {e}")
        else:
            logger.info(f"Deleted {path}")
