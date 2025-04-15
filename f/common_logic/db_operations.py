import logging

from psycopg2 import Error, connect, sql

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


postgresql = dict


def conninfo(db: postgresql):
    """Convert a `postgresql` Windmill Resources to psycopg-style connection string"""
    # password is optional
    password_part = f" password={db['password']}" if "password" in db else ""
    conn = "dbname={dbname} user={user} host={host} port={port}".format(**db)
    return conn + password_part


def fetch_data_from_postgres(db_connection_string, table_name: str):
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
        conn = connect(db_connection_string)
        cursor = conn.cursor()
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
    finally:
        cursor.close()
        conn.close()

    logger.info(f"Data fetched from {table_name}")
    return columns, rows
