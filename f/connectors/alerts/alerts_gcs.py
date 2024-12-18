# requirements:
# google-auth
# google-cloud~=0.34.0
# google-cloud-storage~=2.13
# pandas~=2.2
# pillow~=10.3
# psycopg2-binary
# requests~=2.32

import hashlib
import json
import logging
import uuid
from io import StringIO
from pathlib import Path

import pandas as pd
from numpy import nan
import psycopg2
from psycopg2 import errors, sql
from google.cloud import storage as gcs
from google.oauth2.service_account import Credentials
from PIL import Image

# type names that refer to Windmill Resources
gcp_service_account = dict
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
    gcp_service_acct: gcp_service_account,
    alerts_bucket: str,
    territory_id: int,
    db: postgresql,
    db_table_name: str,
    destination_path: str = "/frizzle-persistent-storage/datalake/change_detection/alerts",
):
    """
    Wrapper around _main() that instantiates the GCP client.
    """
    gcp_credential = Credentials.from_service_account_info(gcp_service_acct)
    storage_client = gcs.Client(
        credentials=gcp_credential, project=gcp_service_acct["project_id"]
    )

    return _main(
        storage_client, alerts_bucket, territory_id, db, db_table_name, destination_path
    )


def _main(
    storage_client: gcs.Client,
    alerts_bucket: str,
    territory_id: int,
    db: postgresql,
    db_table_name: str,
    destination_path: str,
):
    """Download alerts to warehouse storage and index them in a database.

    Parameters
    ----------
    storage_client : google.cloud.storage.Client
    alerts_bucket : str
        The name of the GCS bucket containing alerts.
    territory_id : int
        The ID of the territory for which alerts are being processed.
    db : postgresql
        A dictionary containing database connection parameters.
    db_table_name : str
        The name of the database table to write alerts to.
    destination_path : str, optional
        The local directory to save files

    Returns
    -------
    None
    """
    alerts_metadata_filename = "alerts_history.csv"

    geojson_files, tiff_files, alerts_metadata = sync_gcs_to_local(
        destination_path,
        storage_client,
        alerts_bucket,
        territory_id,
        alerts_metadata_filename,
    )

    convert_tiffs_to_jpg(tiff_files)

    prepared_alerts_metadata = prepare_alerts_metadata(alerts_metadata, territory_id)

    prepared_alerts_data = prepare_alerts_data(destination_path, geojson_files)

    db_writer = AlertsDBWriter(conninfo(db), db_table_name)
    db_writer.handle_output(prepared_alerts_data, prepared_alerts_metadata)
    logger.info(
        f"Alerts data successfully written to database table: [{db_table_name}]"
    )


def _get_rel_filepath(local_file_path, territory_id):
    """Generate the relative file path for a file based on its blob name and local path.

    If the file is an image (e.g., with extensions .tif, .tiff, .jpg, .jpeg),
    'images' will be appended to the path.

    Example
    -------
    If the local_file_path is '/datalake/change_detection/alerts/2023/01/alert_12345.tif'
    and territory_id is 1, the function will return '1/2023/01/12345/images'.

    Parameters
    ----------
    local_file_path : str
        The local file path where the file is stored.
    territory_id : int
        The ID of the territory for which the file is being processed.

    Returns
    -------
    str
        The relative file path for the file.
    """
    path_parts = Path(local_file_path).parts
    year = path_parts[-3]
    month = path_parts[-2]
    filename = path_parts[-1]
    alert_id = filename.split(".")[0].split("_")[-1]
    filepath = Path(str(territory_id), year, month, alert_id)
    if filename.lower().endswith((".tif", ".tiff", ".jpg", ".jpeg")):
        filepath = filepath / "images"
    return str(filepath)


def sync_gcs_to_local(
    destination_path,
    storage_client,
    bucket_name,
    territory_id,
    alerts_metadata_filename,
):
    """Download files from a GCS bucket to a local directory.

    Parameters
    ----------
    destination_path : str
        The local directory where files will be downloaded.
    storage_client : google.cloud.storage.Client
    bucket_name : str
        The name of the GCS bucket to download from.
    territory_id : int
        The path prefix for which files should be downloaded.
    alerts_metadata_filename : str
        An additional file to sync from the GCS bucket.

    Returns
    -------
    tuple
        A tuple containing a set of str with the local file names that were written
        and the alerts metadata content.

    FIXME: sync, don't brute-force re-download files that already exist - see #6
    """

    bucket = storage_client.bucket(bucket_name)

    # List all files in the GCS bucket in territory_id directory
    prefix = f"{territory_id}/"
    files_to_download = set(blob.name for blob in bucket.list_blobs(prefix=prefix))

    assert (
        len(files_to_download) > 0
    ), f"No files found to download in bucket '{bucket_name}' with that prefix."

    logger.info(
        f"Found {len(files_to_download)} files to download from bucket '{bucket_name}'."
    )

    Path(destination_path).mkdir(parents=True, exist_ok=True)

    downloaded_files = set()

    # Download files from the GCS bucket to the local directory
    for blob_name in files_to_download:
        blob = bucket.blob(blob_name)
        local_file_path = Path(destination_path) / blob_name
        filename = local_file_path.name

        logger.info(f"Downloading file: {filename}")

        # Generate relative file path for all files
        rel_filepath = Path(destination_path) / _get_rel_filepath(
            str(local_file_path), territory_id
        )

        if not rel_filepath.exists():
            rel_filepath.mkdir(parents=True, exist_ok=True)

        blob.download_to_filename(rel_filepath / filename)

        downloaded_files.add(str(rel_filepath / filename))

    # Create lists of GeoJSON and TIFF files
    geojson_files = [f for f in downloaded_files if f.endswith(".geojson")]
    tiff_files = [f for f in downloaded_files if f.endswith((".tif", ".tiff"))]

    # Additionally, retrieve alerts metadata content from the root of the bucket
    # and store it in memory (it's not a large file)
    alerts_metadata_blob = bucket.blob(alerts_metadata_filename)
    alerts_metadata = alerts_metadata_blob.download_as_text()

    logger.info("Successfully downloaded files from GCS bucket.")

    return geojson_files, tiff_files, alerts_metadata


def convert_tiffs_to_jpg(tiff_files):
    """Convert TIFF files to JPEG format.

    Parameters
    ----------
    destination_path : str
        The local directory where the `tiff_files` reside.
    files : list of str
        A list of filenames to be processed, which may include TIFF files.
    territory_id : int
        The identifier for the territory used to generate relative file paths.

    Returns
    -------
    None
    """
    logger.info(f"Converting TIF files: {tiff_files}")
    for tiff_file in tiff_files:
        tiff_file_path = Path(tiff_file)
        jpeg_file_path = tiff_file_path.with_suffix(".jpg")
        jpeg_file = jpeg_file_path.name

        # If the jpeg file already exists, skip it
        if jpeg_file_path.exists():
            logger.info(f"JPEG file already exists: {jpeg_file}")
            continue

        logger.info(f"Converting TIFF file to JPEG: {jpeg_file}")

        # Ensure the file exists in the local directory
        if tiff_file_path.exists():
            try:
                with Image.open(tiff_file_path) as img:
                    # Save the image in the same location in the datalake as the tiff
                    img.save(jpeg_file_path, "JPEG")
            except Exception as e:
                logger.error(
                    f"TIFF image can not be opened, potentially empty: {str(e)}"
                )

    logger.info("Successfully converted TIFF files to JPEG.")


def _generate_uuid(row):
    """
    Generate a unique UUID for a row using SHA256 hashing.

    Parameters
    ----------
    row : pd.Series
        A pandas Series representing a row in the DataFrame.

    Returns
    -------
    str
        A unique UUID for the row.
    """

    def sha256sum(data):
        h = hashlib.sha256()
        h.update(data.encode("utf-8"))
        return h.hexdigest()

    row_data = json.dumps(row.to_dict(), sort_keys=True)
    row_hash = sha256sum(row_data)
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, row_hash))


def prepare_alerts_metadata(alerts_metadata, territory_id):
    """
    Prepare alerts metadata by filtering and processing CSV data.

    This function converts CSV data into a DataFrame, filters it based on the
    provided territory_id, and adds additional metadata columns. It generates
    a unique UUID for the metadata based on the content hash and includes a
    placeholder geolocation.

    Parameters
    ----------
    alerts_metadata : str
        CSV data as a string containing alerts metadata.
    territory_id : int
        The identifier for the territory used to filter the metadata.

    Returns
    -------
    list of dict
        A list of dictionaries representing the filtered and processed alerts
        metadata, including additional columns for geolocation, metadata UUID,
        and alert source.
    """
    # Convert CSV bytes to DataFrame
    df = pd.read_csv(StringIO(alerts_metadata))

    # Filter DataFrame based on territory_id
    filtered_df = df[df["territory_id"] == territory_id].copy()

    # Generate a unique UUID for each row
    filtered_df["metadata_uuid"] = filtered_df.apply(_generate_uuid, axis=1)

    # TODO: Currently, this script is only used for Terras alerts. Let's discuss a more sustainable approach with the alerts provider(s).
    filtered_df.loc[:, "alert_source"] = "terras"

    # Replace all NaN values with None
    filtered_df = filtered_df.replace({nan: None})

    # Convert DataFrame to list of dictionaries
    prepared_alerts_metadata = filtered_df.to_dict("records")

    logger.info("Successfully prepared alerts metadata.")

    return prepared_alerts_metadata


def prepare_alerts_data(local_directory, geojson_files):
    """
    Prepare alerts data by reading GeoJSON files from a local directory.

    Parameters
    ----------
    local_directory : str
        The local directory where GeoJSON files are stored.
    geojson_files : list of str
        A list of GeoJSON file names to be processed.

    Returns
    -------
    list of dict
        A list of dictionaries containing the source file name, the GeoJSON data,
        and the alert source.
    """
    prepared_alerts_data = []

    for file_name in geojson_files:
        file_path = Path(local_directory) / file_name
        if file_path.exists():
            logger.info(f"Storing GeoJSON file: {file_name}")
            with file_path.open("r") as f:
                geojson_data = json.load(f)
                # TODO: Currently, this script is only used for Terras alerts. Let's discuss a more sustainable approach with the alerts provider(s).
                prepared_alerts_data.append(
                    {
                        "source": file_name,
                        "data": geojson_data,
                        "alert_source": "terras",
                    }
                )

    logger.info("Successfully prepared alerts data.")
    return prepared_alerts_data


class AlertsDBWriter:
    """
    AlertsDBWriter converts GeoJSON data and alert metadata into structured SQL tables.
    Specifically tailored for operations involving geographic data and alerts metadata stored in GeoJSON format.

    This class manages database connections using PostgreSQL through psycopg2.

    TODO: DRY with KoboDBWriter and CoMapeoDBWriter

    """

    def __init__(self, db_connection_string, table_name):
        """
        Initializes the AlertsDBWriter with the provided connection string and form response table to be used.
        """
        self.db_connection_string = db_connection_string
        self.table_name = table_name

    def _get_conn(self):
        """
        Establishes a connection to the PostgreSQL database using the class's configured connection string.
        """
        return psycopg2.connect(dsn=self.db_connection_string)

    def _create_alerts_metadata_table(self, cursor, table_name):
        metadata_table_name = f"{table_name}__metadata"

        query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {metadata_table_name} (
                _id character varying(36) NOT NULL PRIMARY KEY,
                territory_id text,
                type_alert bigint,
                month bigint,
                year bigint,
                total_alerts bigint,
                description_alerts text,
                confidence real,
                alert_source text
            );
            """).format(metadata_table_name=sql.Identifier(metadata_table_name))
        cursor.execute(query)

    def _create_alerts_table(self, cursor, table_name):
        query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {table_name}
        (
            _id character varying(36) NOT NULL PRIMARY KEY,
            -- These are found in "properties" of an alert Feature:
            alert_type text,
            area_alert_ha double precision,  -- only present for polygon
            basin_id bigint,
            confidence real,
            count bigint,
            date_end_t0 text,
            date_end_t1 text,
            date_start_t0 text,
            date_start_t1 text,
            grid bigint,
            label bigint,
            month_detec text,
            sat_detect_prefix text,
            sat_viz_prefix text,
            satellite text,
            territory_id bigint,
            territory_name text,
            year_detec text,
            length_alert_km double precision,  -- only present for linestring
            -- Deconstruct the "geometry" of a Feature:
            g__type text,
            g__coordinates text,
            -- Added by us
            source text,
            alert_source text
        );
        """).format(table_name=sql.Identifier(table_name))
        cursor.execute(query)

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

    def handle_output(self, alerts, alerts_metadata):
        """
        Inserts alerts and metadata from GeoJSON data into a PostgreSQL database.

        This method processes a list of alerts and their corresponding metadata, extracting relevant features and properties from each GeoJSON object. It constructs and executes SQL queries to insert these data into the appropriate database tables. The method ensures that each alert and metadata entry is processed within a transaction, committing the transaction upon successful insertion or rolling back in case of errors.

        Parameters
        ----------
        alerts (list): A list of alert objects, each containing GeoJSON data with features and properties to be inserted into the database.
        alerts_metadata (list): A list of metadata objects associated with the alerts, containing additional information to be stored in a separate metadata table.
        """
        table_name = self.table_name
        conn = self._get_conn()
        cursor = conn.cursor()

        try:
            if alerts:
                self._create_alerts_table(cursor, table_name)
            else:
                logger.info("No alerts data to store.")

            for alert in alerts:
                try:
                    cursor.execute("BEGIN")

                    for feature in alert["data"]["features"]:
                        try:
                            source = alert["source"]
                            alert_source = alert["alert_source"]
                            if "properties" in feature:
                                properties_str = json.dumps(feature["properties"])
                                properties = json.loads(properties_str)
                            else:
                                properties = None

                            _id = properties.get("id")
                            alert_type = properties.get("alert_type")
                            area_alert_ha = properties.get("area_alert_ha")
                            basin_id = properties.get("basin_id")
                            confidence = properties.get("confidence")
                            count = properties.get("count")
                            date_end_t0 = properties.get("date_end_t0")
                            date_end_t1 = properties.get("date_end_t1")
                            date_start_t0 = properties.get("date_start_t0")
                            date_start_t1 = properties.get("date_start_t1")
                            grid = properties.get("grid")
                            label = properties.get("label")
                            month_detec = properties.get("month_detec")
                            sat_detect_prefix = properties.get("sat_detect_prefix")
                            sat_viz_prefix = properties.get("sat_viz_prefix")
                            satellite = properties.get("satellite")
                            territory_id = properties.get("territory_id")
                            territory_name = properties.get("territory_name")
                            year_detec = properties.get("year_detec")
                            # In lieu of, say, PostGIS, use `g__*` columns to represent the Feature's geometry.
                            g__type = feature["geometry"].get("type")
                            g__coordinates = json.dumps(
                                feature["geometry"]["coordinates"]
                            )
                            length_alert_km = properties.get("length_alert_km")

                            columns = [
                                "_id",
                                "alert_type",
                                "area_alert_ha",
                                "basin_id",
                                "confidence",
                                "count",
                                "date_end_t0",
                                "date_end_t1",
                                "date_start_t0",
                                "date_start_t1",
                                "grid",
                                "label",
                                "month_detec",
                                "sat_detect_prefix",
                                "sat_viz_prefix",
                                "satellite",
                                "territory_id",
                                "territory_name",
                                "year_detec",
                                "source",
                                "g__type",
                                "g__coordinates",
                                "length_alert_km",
                                "alert_source",
                            ]

                            values = [
                                _id,
                                alert_type,
                                area_alert_ha,
                                basin_id,
                                confidence,
                                count,
                                date_end_t0,
                                date_end_t1,
                                date_start_t0,
                                date_start_t1,
                                grid,
                                label,
                                month_detec,
                                sat_detect_prefix,
                                sat_viz_prefix,
                                satellite,
                                territory_id,
                                territory_name,
                                year_detec,
                                source,
                                g__type,
                                g__coordinates,
                                length_alert_km,
                                alert_source,
                            ]

                            self._safe_insert(cursor, table_name, columns, values)

                        except errors.UniqueViolation:
                            logger.info(
                                f"Skipping insert due to UniqueViolation, this entry has been processed already in the past: {_id}"
                            )
                            continue
                        except Exception:
                            logger.exception(
                                "An unexpected error occurred while processing feature"
                            )
                            raise
                    conn.commit()
                except Exception as e:
                    logger.exception(
                        f"An error occurred while processing alerts: {str(e)}"
                    )
                    conn.rollback()
                    raise

            if alerts_metadata:
                self._create_alerts_metadata_table(cursor, table_name)
            else:
                logger.info("No alerts metadata to store.")

            for metadata in alerts_metadata:
                try:
                    cursor.execute("BEGIN")

                    _id = metadata.get("metadata_uuid")
                    territory_id = metadata.get("territory_id")
                    type_alert = metadata.get("type_alert")
                    month = metadata.get("month")
                    year = metadata.get("year")
                    total_alerts = metadata.get("total_alerts")
                    description_alerts = metadata.get("description_alerts")
                    confidence = metadata.get("confidence")
                    alert_source = metadata.get("alert_source")

                    columns = [
                        "_id",
                        "territory_id",
                        "type_alert",
                        "month",
                        "year",
                        "total_alerts",
                        "description_alerts",
                        "confidence",
                        "alert_source",
                    ]

                    values = [
                        _id,
                        territory_id,
                        type_alert,
                        month,
                        year,
                        total_alerts,
                        description_alerts,
                        confidence,
                        alert_source,
                    ]

                    self._safe_insert(
                        cursor, f"{table_name}__metadata", columns, values
                    )
                    conn.commit()
                except errors.UniqueViolation:
                    logger.info(
                        f"Skipping insert due to UniqueViolation, this entry has been processed already in the past: {_id}"
                    )
                    conn.rollback()
                except Exception as e:
                    logger.exception(
                        f"An error occurred while processing alerts metadata: {str(e)}"
                    )
                    conn.rollback()
                    raise

        finally:
            cursor.close()
            conn.close()
