# requirements:
# psycopg[binary]

import csv
import logging
from pathlib import Path

from f.common_logic.db_operations import StructuredDBWriter, conninfo, postgresql

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(
    db: postgresql,
    db_table_name: str,
    csv_path: str,
    attachment_root: str = "/persistent-storage/datalake/",
    delete_csv_file: bool = False,
    id_column: str = None,
):
    """
    Import CSV data into PostgreSQL table.

    Parameters
    ----------
    db : postgresql
        Database connection object.
    db_table_name : str
        Name of the database table to create/insert into.
    csv_path : str
        Path to the CSV file to import.
    attachment_root : str
        Root directory where CSV file is located.
    delete_csv_file : bool
        Whether to delete the CSV file after processing.
    id_column : str, optional
        Name of column to use as primary key. If None, auto-generates _id.
    """
    csv_path = Path(attachment_root) / Path(csv_path)
    transformed_csv_data = transform_csv_data(csv_path, id_column)

    db_writer = StructuredDBWriter(
        conninfo(db),
        db_table_name,
        use_mapping_table=False,
        reverse_properties_separated_by=None,
    )
    db_writer.handle_output(transformed_csv_data)

    if delete_csv_file:
        delete_csv_file(csv_path)


def transform_csv_data(csv_path, id_column=None):
    """
    Transform CSV data into a list of dictionaries suitable for database insertion.

    Parameters
    ----------
    csv_path : str or Path
        Path to the CSV file to read.
    id_column : str, optional
        Name of column to use as primary key. If None, auto-generates _id.

    Returns
    -------
    list
        List of dictionaries where each dictionary represents a CSV row with keys
        matching column names, and an '_id' field for the primary key.
    """
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    transformed_csv_data = []
    for idx, row in enumerate(rows, 1):
        # Use specified column as _id, or generate auto-incrementing _id
        if id_column and id_column in row:
            row_id = row[id_column]
            # Remove the original id column since we're using it as _id
            if id_column != "_id":
                del row[id_column]
        else:
            row_id = str(idx)

        transformed_row = {
            "_id": row_id,
            **row,
        }
        transformed_csv_data.append(transformed_row)

    return transformed_csv_data


def delete_csv_file(csv_path: Path):
    """
    Delete the CSV file after processing.

    Parameters
    ----------
    csv_path : Path
        Path to the CSV file to delete.
    """
    try:
        csv_path.unlink()
        logger.info(f"Deleted CSV file: {csv_path}")
    except FileNotFoundError:
        logger.warning(f"CSV file not found: {csv_path}")
    except Exception as e:
        logger.error(f"Error deleting CSV file: {e}")
        raise
