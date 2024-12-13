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
import os
import uuid

import pandas as pd
import psycopg2
from google.cloud import storage as gcs
from google.oauth2.service_account import Credentials
from PIL import Image
from psycopg2 import errors, sql

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

    downloaded_files = sync_gcs_to_local(
        destination_path,
        storage_client,
        alerts_bucket,
        territory_id,
        alerts_metadata_filename,
    )
    gjson_and_metas = process_files(
        destination_path, downloaded_files, territory_id, alerts_metadata_filename
    )
    outputs = load_alerts_to_postgis(destination_path, gjson_and_metas)

    db_writer = AlertsDBWriter(conninfo(db), db_table_name)
    db_writer.handle_output(outputs["geojsons"], outputs["alerts_metadata"])
    logger.info(f"Wrote response content to database table [{db_table_name}]")


def _get_tif_rel_filepath(blob_name, local_file_path, territory_id):
    """Generate the relative file path for a TIF file based on its blob name and local path.

    Example
    -------
    If the local_file_path is '/datalake/change_detection/alerts/2023/01/alert_12345.tif'
    and territory_id is 1, the function will return '1/2023/01/12345/images'.

    Parameters
    ----------
    blob_name : str
        ignored - TODO remove
    local_file_path : str
        The local file path where the file is stored.
    territory_id : int
        The ID of the territory for which the file is being processed.

    Returns
    -------
    str
        The relative file path for the TIF file.
    """
    year = local_file_path.split("/")[-3]
    month = local_file_path.split("/")[-2]
    filename = local_file_path.split("/")[-1]
    alert_id = filename.split(".")[0].split("_")[-1]
    filepath = os.path.join(str(territory_id), year, month, alert_id, "images")
    return filepath


def sync_gcs_to_local(
    dst_path, storage_client, bucket_name, territory_id, alerts_metadata_filename
):
    """Download files from a GCS bucket to a local directory.

    Parameters
    ----------
    dst_path : str
        The local directory where files will be downloaded.
    storage_client : google.cloud.storage.Client
    bucket_name : str
        The name of the GCS bucket to download from.
    territory_id : int
        The path prefix for which files shuold be downloaded.
    alerts_metadata_filename : str
        An additional file to download: mean tto be the filename containing alerts metadata.

    Returns
    -------
    set of str
        the (local) file names that were written.

    FIXME: Ensure dst_path exists before downloading files.
    FIXME: sync, don't brute-force re-download files that already exist - see #6
    """
    bucket = storage_client.bucket(bucket_name)

    # List all files in the GCS bucket
    prefix = f"{territory_id}/"
    files_to_download = set(blob.name for blob in bucket.list_blobs(prefix=prefix))

    assert (
        len(files_to_download) > 0
    ), f"No files found to download in bucket '{bucket_name}' with that prefix."

    # Add metadata csv file at the bucket level
    # TODO: once we have SAs per community and folder, they would need object level read access for this file
    files_to_download.add(alerts_metadata_filename)

    logger.info(
        f"Found {len(files_to_download)} files to download from bucket '{bucket_name}'."
    )

    # Download new or updated files from the GCS bucket to the local directory
    for blob_name in files_to_download:
        blob = bucket.blob(blob_name)
        local_file_path = os.path.join(dst_path, blob_name)
        filename = local_file_path.split("/")[-1]

        logger.info(f"Downloading file: {filename}")

        if filename.lower().endswith(".tif") or filename.lower().endswith(".tiff"):
            rel_filepath = os.path.join(
                dst_path,
                _get_tif_rel_filepath(blob_name, local_file_path, territory_id),
            )
        # if non-tif such as csv or geojson
        else:
            rel_filepath = os.path.dirname(local_file_path)

        if not os.path.exists(rel_filepath):
            os.makedirs(rel_filepath)

        blob.download_to_filename(os.path.join(rel_filepath, filename))

    return files_to_download


def sha256sum(filename, bufsize=128 * 1024):
    """
    Calculate the SHA-256 checksum of a file to ensure that `process_files` asset processes only new versions of `alerts_metadata.csv` (filename).

    This function reads the file in chunks to avoid loading the entire file into memory, which is particularly useful for large files.

    Parameters
    ----------
    filename : str
        The path to the file whose checksum needs to be calculated.
    bufsize : int (optional)
        The size of the buffer used for reading the file in chunks. Default is 128 KB.

    Returns
    -------
    str
        The hexadecimal representation of the SHA-256 checksum of the file.
    """
    h = hashlib.sha256()
    buffer = bytearray(bufsize)
    buffer_view = memoryview(buffer)
    with open(filename, "rb", buffering=0) as f:
        while True:
            n = f.readinto(buffer_view)
            if not n:
                break
            h.update(buffer_view[:n])
    return h.hexdigest()


def process_files(path, files, territory_id, alerts_metadata_filename):
    """Convert various `files`, specifically GeoJSON, TIFF, and CSV files.

    1. Converts any TIFF file to JPEG format (but also keeping the TIFF file)
    2. Collects the names of any .geojson file in path and returns it.
    3. Filters the `alerts_metadata_filename` CSV rows based on the specified territory ID,
      and generates a unique metadata UUID for the filtered CSV records, returning them.

    Parameters
    ----------
    path : str
        The local directory path where the `files` reside.
    files : list of str
        A list of filenames to be processed, which may include GeoJSON, TIFF, and CSV files.
    territory_id : int
        The identifier for the territory used to filter the alerts metadata.
    alerts_metadata_filename : str
        The filename of the alerts metadata CSV that needs to be processed.

    Returns
    -------
    dict
        contains keys
        - "geojsons": A list of the GeoJSON filenames found in `files`.
        - "alerts_metadata":
            A list of filtered alerts metadata records: [{column -> value}, … , {column -> value}]
            or None if no valid records were found.

    FIXME: Refactor this function for better readability and separation of concerns.
    """
    logger.info(f"Processing files: {files}")
    geojsons = []
    alerts_metadata = None
    for file_name in files:
        if file_name.endswith(".geojson"):
            geojsons.append(file_name)
        elif file_name.endswith(".tif"):
            jpeg_file = file_name.split("/")[-1].split(".")[0] + ".jpg"

            local_file_path = os.path.join(path, file_name)
            rel_filepath = os.path.join(
                path,
                _get_tif_rel_filepath(file_name, local_file_path, territory_id),
            )

            # if jpeg file already exist skip it
            if os.path.exists(os.path.join(rel_filepath, jpeg_file)):
                continue

            logger.info(f"Converting TIFF file to JPEG: {jpeg_file}")

            # Ensure the file exists in the local directory
            if os.path.exists(os.path.join(rel_filepath, file_name.split("/")[-1])):
                try:
                    with Image.open(
                        os.path.join(rel_filepath, file_name.split("/")[-1])
                    ) as img:
                        # save the image at the same location in the datalake as its tiff
                        img.save(os.path.join(rel_filepath, jpeg_file), "JPEG")
                except Exception as e:
                    logger.error(
                        f"Tif image can not be opened, potentially empty: {str(e)}"
                    )
        elif file_name.endswith(".csv"):
            if file_name != alerts_metadata_filename:
                continue
            file_path = os.path.join(path, file_name)
            if os.path.exists(file_path):
                df = pd.read_csv(file_path)
                filtered_df = df[df["territory_id"] == territory_id]
                # Hash the file content
                hash_hex = sha256sum(file_path)
                # Generate a UUID from the content hash
                metadata_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, hash_hex)

                filtered_df["_geolocation"] = [[0.0, 0.0]] * len(filtered_df)
                filtered_df["metadata_uuid"] = [str(metadata_uuid)] * len(filtered_df)
                filtered_df["source"] = file_name
                # this op is used for terras alerts only at this point
                filtered_df["alert_source"] = "terras"
                alerts_metadata = filtered_df.to_dict("records")
        else:
            logger.info(f"Skipping file: {file_name}")

    return {"geojsons": geojsons, "alerts_metadata": alerts_metadata}


def load_alerts_to_postgis(local_directory, processed):
    outputs = {}
    outputs["alerts_metadata"] = processed["alerts_metadata"]
    outputs["geojsons"] = []

    for file_name in processed["geojsons"]:
        if os.path.exists(os.path.join(local_directory, file_name)):
            logger.info(f"Storing GeoJSON file: {file_name}")
            with open(os.path.join(local_directory, file_name), "r") as f:
                geojson_data = json.load(f)
                # this op is used for terras alerts only at this point to have at single alert level
                outputs["geojsons"].append(
                    {
                        "source": file_name,
                        "data": geojson_data,
                        "alert_source": "terras",
                    }
                )

    return outputs


class AlertsDBWriter:
    """
    AlertsDBWriter converts GeoJSON data and alert metadata into structured SQL tables.
    Specifically tailored for operations involving geographic data and alerts metadata stored in GeoJSON format.

    This class manages database connections using PostgreSQL through psycopg2.

    TODO: DRY with KoboDBWriter

    """

    def __init__(self, db_connection_string, table_name):
        """
        Initializes the GeojsonIOManager with the provided connection string and form response table to be used
        """
        self.db_connection_string = db_connection_string
        self.table_name = table_name

    def _get_conn(self):
        """
        Establishes a connection to the PostgreSQL database using the class's configured connection string.
        """
        return psycopg2.connect(dsn=self.db_connection_string)

    def _create_metadata_cols(self, metadata, table_name):
        metadata_table_name = f"{table_name}__metadata"

        with self._get_conn() as conn, conn.cursor() as cursor:
            query = sql.SQL("""
                CREATE TABLE IF NOT EXISTS {metadata_table_name} (
                    territory_id text NOT NULL,
                    type_alert text NOT NULL,
                    month text NOT NULL,
                    year text NOT NULL,
                    total_alerts text NOT NULL,
                    description_alerts text,
                    confidence smallint,
                    metadata_uuid text,
                    source text,
                    alert_source text
                );
                """).format(metadata_table_name=sql.Identifier(metadata_table_name))
            cursor.execute(query)
            conn.commit()

    def _create_alerts_table(self, table_name):
        with self._get_conn() as conn, conn.cursor() as cursor:
            query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {table_name}
            (
                _id character varying(36) NOT NULL,
                -- These are found in "properties" of an alert Feature:
                alert_type text,
                area_alert_ha double precision,  -- only present for polygon
                basin_id bigint,
                confidence smallint,
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
            conn.commit()

    def handle_output(self, geojsons, metadatas):
        """
        Processes GeoJSON data/metadata from Dagster assets and inserts it into a PostgreSQL database. It iterates over each GeoJSON/metadata object, extracts relevant features and properties, and constructs SQL queries to insert these data into the database. After processing all features, it commits the transaction and closes the database connection.
        """
        table_name = self.table_name
        conn = self._get_conn()
        cursor = conn.cursor()

        try:
            if geojsons:
                # check to see if we need to create the table for the first time
                # FIXME: this func creates its own cursor but we already have one
                self._create_alerts_table(table_name)
            else:
                logger.info("No alerts geojson to store.")

            for geojson in geojsons:
                try:
                    cursor.execute("BEGIN")  # Start a new transaction

                    for feature in geojson["data"]["features"]:
                        try:
                            source = geojson["source"]
                            alert_source = geojson["alert_source"]
                            if "properties" in feature:
                                properties_str = json.dumps(feature["properties"])
                                properties = json.loads(properties_str)
                            else:
                                properties = None

                            # Safely accessing each property, defaulting to None if missing
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

                            # Inserting data into the alerts table
                            query = f"""INSERT INTO {table_name} (_id, alert_type, area_alert_ha, basin_id, confidence, count, date_end_t0, date_end_t1, date_start_t0, date_start_t1, grid, label, month_detec, sat_detect_prefix, sat_viz_prefix, satellite, territory_id, territory_name, year_detec, source, g__type, g__coordinates, length_alert_km, alert_source) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

                            # Execute the query
                            cursor.execute(
                                query,
                                (
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
                                ),
                            )

                        except errors.UniqueViolation:
                            logger.info(
                                f"Skipping insert due to UniqueViolation, this alert has been processed already in the past: {source}"
                            )
                            continue
                        except Exception:
                            logger.exception(
                                "An unexpected error occurred while processing feature"
                            )
                            raise
                    # End of inner loop, commit the transaction after processing all features
                    conn.commit()
                except Exception:
                    logger.exception("An error occurred while processing GeoJSON")
                    conn.rollback()  # Rollback the transaction in case of an error
                    raise

            if metadatas:
                self._create_metadata_cols(metadatas[0], table_name)
            else:
                logger.info("No alerts metadata to store.")

            for metadata in metadatas:
                try:
                    cursor.execute("BEGIN")  # Start a new transaction
                    source = metadata["source"]

                    territory_id = metadata.get("territory_id")
                    type_alert = metadata.get("type_alert")
                    month = metadata.get("month")
                    year = metadata.get("year")
                    total_alerts = metadata.get("total_alerts")
                    description_alerts = metadata.get("description_alerts")
                    confidence = metadata.get("confidence")
                    metadata_uuid = metadata.get("metadata_uuid")
                    source = metadata.get("source")
                    alert_source = metadata.get("alert_source")

                    if confidence is not None:
                        try:
                            confidence = int(confidence)
                        except ValueError:
                            confidence = None

                    # Inserting data into the configured table with _metadata prepend name
                    query = f"""INSERT INTO {table_name}__metadata (territory_id, type_alert, month, year, total_alerts, description_alerts, confidence, metadata_uuid, source, alert_source) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

                    # Execute the query
                    cursor.execute(
                        query,
                        (
                            territory_id,
                            type_alert,
                            month,
                            year,
                            total_alerts,
                            description_alerts,
                            confidence,
                            metadata_uuid,
                            source,
                            alert_source,
                        ),
                    )

                    # End of inner loop, commit the transaction after processing all features
                    conn.commit()
                except errors.UniqueViolation:
                    logger.info(
                        f"Skipping insert due to UniqueViolation, this alert has been processed already in the past: {source}"
                    )
                    conn.rollback()
                except Exception:
                    logger.exception(
                        "An error occurred while processing alerts metadata."
                    )
                    conn.rollback()  # Rollback the transaction in case of an error
                    raise

        finally:
            cursor.close()
            conn.close()
