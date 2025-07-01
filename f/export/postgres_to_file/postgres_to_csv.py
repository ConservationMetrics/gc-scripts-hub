import logging
from pathlib import Path

from psycopg2 import connect, sql

from f.common_logic.db_operations import conninfo

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(
    db: dict,
    db_table_name: str,
    storage_path: str = "/persistent-storage/datalake/export",
):
    """
    Export a PostgreSQL table to a CSV file using the COPY command.

    This function uses psycopg2's `copy_expert` method to execute a SQL COPY command
    that exports the specified table to a CSV file. For more details on `copy_expert`,
    see the documentation: https://www.psycopg.org/docs/cursor.html#cursor.copy_expert

    Parameters
    ----------
    db : dict
        A dictionary containing database connection parameters.
    db_table_name : str
        The name of the table to export.
    storage_path : str, optional
        The directory path where the CSV file will be saved. Defaults to
        "/persistent-storage/datalake/export".
    """
    out_path = Path(storage_path) / f"{db_table_name}.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    conn_str = conninfo(db)

    try:
        with (
            connect(conn_str) as conn,
            conn.cursor() as cur,
            out_path.open("w", encoding="utf-8") as f,
        ):
            copy_sql = sql.SQL(
                "COPY {} TO STDOUT WITH CSV HEADER QUOTE '\"' DELIMITER ',' NULL ''"
            ).format(sql.Identifier(db_table_name))
            cur.copy_expert(copy_sql, f)
            logger.info(f"Exported {db_table_name} to {out_path}")
    except Exception as e:
        logger.error(f"Failed to export {db_table_name} to CSV: {e}")
        raise
