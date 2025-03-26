# requirements:
# google-auth
# google-cloud~=0.34.0
# google-cloud-storage~=2.13
# pandas~=2.2
# pillow~=10.3
# psycopg2-binary
# requests~=2.32

import base64
import hashlib
import json
import logging
import uuid
from io import StringIO
from pathlib import Path

import pandas as pd
import psycopg2
from google.cloud import storage as gcs
from google.oauth2.service_account import Credentials
from PIL import Image
from psycopg2 import sql

from f.common_logic.db_connection import conninfo, postgresql

# type names that refer to Windmill Resources
gcp_service_account = dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(
    gcp_service_acct: gcp_service_account,
    alerts_bucket: str,
    territory_id: int,
    db: postgresql,
    db_table_name: str,
    destination_path: str = "/persistent-storage/datalake/change_detection/alerts",
):
    """
    Wrapper around _main() that instantiates the GCP client.
    """
    gcp_credential = Credentials.from_service_account_info(gcp_service_acct)
    storage_client = gcs.Client(
        credentials=gcp_credential, project=gcp_service_acct["project_id"]
    )

    return _main(
        storage_client,
        alerts_bucket,
        territory_id,
        db,
        db_table_name,
        destination_path,
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

    prepared_alerts_metadata, alerts_statistics = prepare_alerts_metadata(
        alerts_metadata, territory_id
    )

    prepared_alerts_data = prepare_alerts_data(destination_path, geojson_files)

    db_writer = AlertsDBWriter(conninfo(db), db_table_name)
    new_alerts_data = db_writer.handle_output(
        prepared_alerts_data, prepared_alerts_metadata
    )

    if new_alerts_data:
        return alerts_statistics


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
    geojson_files : set of str
        A set containing the local file paths of the downloaded GeoJSON files.
    tiff_files : set of str
        A set containing the local file paths of the downloaded TIFF files.
    alerts_metadata : str
        The content of the alerts metadata file.

    Notes
    -----
    The function checks the last modified timestamps of both the local file and the GCS file.
    If the local file is up-to-date or newer than the GCS file, it will skip downloading the file.
    """

    destination_path = Path(destination_path)

    bucket = storage_client.bucket(bucket_name)

    # List all files in the GCS bucket in territory_id directory
    prefix = f"{territory_id}/"
    files_to_download = set(blob.name for blob in bucket.list_blobs(prefix=prefix))

    assert len(files_to_download) > 0, (
        f"No files found to download in bucket '{bucket_name}' with that prefix."
    )

    logger.info(
        f"Found {len(files_to_download)} files to download from bucket '{bucket_name}'."
    )

    destination_path.mkdir(parents=True, exist_ok=True)

    geojson_files = set()
    tiff_files = set()

    # Filter files to download only geojson and tiff files
    files_to_download = {
        blob_name
        for blob_name in files_to_download
        if blob_name.lower().endswith((".geojson", ".tif", ".tiff"))
    }

    # Download files from the GCS bucket to the local directory
    for blob_name in files_to_download:
        blob = bucket.blob(blob_name)
        local_file_path = destination_path / blob_name
        filename = local_file_path.name

        # Generate relative file path for all files
        rel_filepath = destination_path / _get_rel_filepath(
            str(local_file_path), territory_id
        )

        local_file_full_path = rel_filepath / filename

        if local_file_full_path.exists():
            # Get the local file's last modified time
            with open(local_file_full_path, "rb") as f:
                local_md5_hash = hashlib.md5(f.read()).hexdigest()

            # Get the GCS file's MD5 hash
            blob.reload()
            gcs_md5_hash_base64 = blob.md5_hash

            # GCP's MD5 hash is base64-encoded and needs to be decoded
            gcs_md5_hash = base64.b64decode(gcs_md5_hash_base64).hex()

            if local_md5_hash == gcs_md5_hash:
                logger.debug(f"File is up-to-date, skipping download: {filename}")
                continue

            blob = bucket.blob(blob_name)

        logger.info(f"Downloading file: {filename}")
        if not rel_filepath.exists():
            rel_filepath.mkdir(parents=True, exist_ok=True)
        blob.download_to_filename(local_file_full_path)

        file_path_str = str(local_file_full_path)
        if file_path_str.endswith(".geojson"):
            geojson_files.add(file_path_str)
        elif file_path_str.endswith((".tif", ".tiff")):
            tiff_files.add(file_path_str)

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


def prepare_alerts_metadata(alerts_metadata, territory_id):
    """
    Prepare alerts metadata by filtering and processing CSV data.

    This function converts CSV data into a DataFrame, filters it based on the
    provided territory_id, and adds additional metadata columns. It generates
    a unique UUID for the metadata based on the content hash and includes a
    placeholder geolocation.

    The alert statistics dictionary is generated by summing up the total alerts
    and concatenating unique description alerts for the latest month and year.

    Parameters
    ----------
    alerts_metadata : str
        CSV data as a string containing alerts metadata.
    territory_id : int
        The identifier for the territory used to filter the metadata.

    Returns
    -------
    prepared_alerts_metadata : list of dict
        A list of dictionaries representing the filtered and processed alerts
        metadata, including additional columns for geolocation, metadata UUID,
        and alert source.
    alerts_statistics : dict
        A dictionary containing alert statistics: total alerts, month/year,
        and description of alerts.
    """
    # Convert CSV bytes to DataFrame and filter based on territory_id
    filtered_df = pd.read_csv(StringIO(alerts_metadata)).query(
        "territory_id == @territory_id"
    )

    # Hash each row into a unique UUID; this will be used as the primary key for the metadata table
    # The hash is based on the most important columns for the metadata table, so that changes in other columns do not affect the hash
    filtered_df["metadata_uuid"] = pd.util.hash_pandas_object(
        filtered_df[
            [
                "territory_id",
                "month",
                "year",
                "description_alerts",
            ]
        ].sort_index(axis=1),
        index=False,
    )

    # TODO: Currently, this script is only used for Terras alerts. Let's discuss a more sustainable approach with the alerts provider(s).
    # Also, if this changes for future alerts, we will need to ensure that existing records are not overwritten.
    filtered_df["data_source"] = "terras"

    # Replace all NaN values with None
    filtered_df.replace({float("nan"): None}, inplace=True)

    # Convert DataFrame to list of dictionaries
    prepared_alerts_metadata = filtered_df.to_dict("records")

    logger.info("Successfully prepared alerts metadata.")

    # Determine latest month and year
    latest_month_year = (
        filtered_df[["month", "year"]]
        .drop_duplicates()
        .sort_values(by=["year", "month"], ascending=[False, False])
        .iloc[0]
    )

    # Filter for rows matching the latest month and year
    latest_rows = filtered_df[
        (filtered_df["month"] == latest_month_year["month"])
        & (filtered_df["year"] == latest_month_year["year"])
    ]

    # Generate alert statistics for the latest month and year
    total_alerts = latest_rows["total_alerts"].sum()
    description_alerts = ", ".join(
        latest_rows["description_alerts"].drop_duplicates().str.replace("_", " ")
    )
    alerts_statistics = {
        "total_alerts": str(total_alerts),
        "month_year": f"{latest_month_year['month']}/{latest_month_year['year']}",
        "description_alerts": description_alerts,
    }

    return prepared_alerts_metadata, alerts_statistics


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
                # TODO: See comment above about data source
                prepared_alerts_data.append(
                    {
                        "data": geojson_data,
                        "data_source": "terras",
                        "source_file_name": file_name,
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

    def _create_alerts_metadata_table(self, table_name):
        metadata_table_name = f"{table_name[:53]}__metadata"

        with self._get_conn() as conn, conn.cursor() as cursor:
            query = sql.SQL("""
                CREATE TABLE IF NOT EXISTS {metadata_table_name} (
                    _id character varying(36) NOT NULL PRIMARY KEY,
                    confidence real,
                    description_alerts text,
                    month bigint,
                    territory_id bigint,
                    total_alerts bigint,
                    type_alert bigint,
                    year bigint,
                    data_source text
                );
                """).format(metadata_table_name=sql.Identifier(metadata_table_name))
            cursor.execute(query)
            conn.commit()

    def _create_alerts_table(self, table_name):
        with self._get_conn() as conn, conn.cursor() as cursor:
            query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {table_name}
            (
                _id uuid PRIMARY KEY,
                -- These are found in "properties" of an alert Feature:
                alert_id text,
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
                data_source text,
                source_file_name text
            );
            """).format(table_name=sql.Identifier(table_name))
            cursor.execute(query)
            conn.commit()

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

        new_alerts_data = False
        data_inserted_count = 0
        data_updated_count = 0
        metadata_inserted_count = 0
        metadata_updated_count = 0

        try:
            if alerts:
                self._create_alerts_table(table_name)
            else:
                logger.info("No alerts data to store.")

            for alert in alerts:
                try:
                    cursor.execute("BEGIN")

                    for feature in alert["data"]["features"]:
                        try:
                            if "properties" in feature:
                                properties_str = json.dumps(feature["properties"])
                                properties = json.loads(properties_str)
                            else:
                                properties = None

                            # Generate a UUID for the alert based on the alert's ID
                            _id = str(
                                uuid.uuid5(uuid.NAMESPACE_DNS, properties.get("id"))
                            )
                            alert_id = properties.get("id")
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
                            length_alert_km = properties.get("length_alert_km")
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
                            data_source = alert["data_source"]
                            source_file_name = alert["source_file_name"]

                            columns = [
                                "_id",
                                "alert_id",
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
                                "g__type",
                                "g__coordinates",
                                "length_alert_km",
                                "data_source",
                                "source_file_name",
                            ]

                            values = [
                                _id,
                                alert_id,
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
                                g__type,
                                g__coordinates,
                                length_alert_km,
                                data_source,
                                source_file_name,
                            ]

                            result_inserted_count, result_updated_count = (
                                self._safe_insert(cursor, table_name, columns, values)
                            )
                            data_inserted_count += result_inserted_count
                            data_updated_count += result_updated_count

                        except Exception:
                            logger.exception(
                                "An unexpected error occurred while processing feature"
                            )
                            raise
                    conn.commit()
                except Exception:
                    logger.exception("An error occurred while processing GeoJSON")
                    conn.rollback()
                    raise

            if alerts_metadata:
                self._create_alerts_metadata_table(table_name)
            else:
                logger.info("No alerts metadata to store.")

            for metadata in alerts_metadata:
                try:
                    cursor.execute("BEGIN")

                    _id = metadata.get("metadata_uuid")
                    confidence = metadata.get("confidence")
                    description_alerts = metadata.get("description_alerts")
                    month = metadata.get("month")
                    territory_id = metadata.get("territory_id")
                    total_alerts = metadata.get("total_alerts")
                    type_alert = metadata.get("type_alert")
                    year = metadata.get("year")
                    data_source = metadata.get("data_source")

                    columns = [
                        "_id",
                        "territory_id",
                        "type_alert",
                        "month",
                        "year",
                        "total_alerts",
                        "description_alerts",
                        "confidence",
                        "data_source",
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
                        data_source,
                    ]

                    result_inserted_count, result_updated_count = self._safe_insert(
                        cursor,
                        f"{table_name}__metadata",
                        columns,
                        values,
                    )
                    metadata_inserted_count += result_inserted_count
                    metadata_updated_count += result_updated_count

                    conn.commit()
                except Exception:
                    logger.exception(
                        "An error occurred while processing alerts metadata."
                    )
                    conn.rollback()
                    raise

        finally:
            logger.info(f"Total alert rows inserted: {data_inserted_count}")
            logger.info(f"Total alert rows updated: {data_updated_count}")
            logger.info(f"Total metadata rows inserted: {metadata_inserted_count}")
            logger.info(f"Total metadata rows updated: {metadata_updated_count}")
            cursor.close()
            conn.close()

        if data_inserted_count > 0 or metadata_inserted_count > 0:
            new_alerts_data = True
        return new_alerts_data
