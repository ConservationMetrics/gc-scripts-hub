import logging

from f.common_logic.db_operations import conninfo, postgresql

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(db: postgresql, table_name, labels_to_apply):
    """
    Create a Local Contexts label lookup table for a dataset.

    The output table is named `{table_name}__lc_labels` and contains:
    - `_id`: integer primary key starting at 0
    - `label`: Local Contexts label value

    If the table already exists, it is dropped and recreated.

    Parameters
    ----------
    db : postgresql
        Database connection resource used to access the Postgres instance.
    table_name : str
        Base dataset table name.
    labels_to_apply : list[str]
        List of Local Contexts label values to store.

    Returns
    -------
    str
        Name of the created Local Contexts label table.
    """
    import psycopg
    from psycopg import sql

    output_table = f"{table_name}__lc_labels"
    labels_to_apply = labels_to_apply or []

    with psycopg.connect(conninfo(db)) as conn:
        with conn.cursor() as cur:
            logger.info(f"Recreating Local Contexts label table: {output_table}")

            # Drop existing table so apply behavior is a full overwrite
            cur.execute(
                sql.SQL("DROP TABLE IF EXISTS {}").format(
                    sql.Identifier(output_table)
                )
            )

            # Create fresh lookup table
            cur.execute(
                sql.SQL(
                    """
                    CREATE TABLE {} (
                        _id INTEGER PRIMARY KEY,
                        label TEXT NOT NULL
                    )
                    """
                ).format(sql.Identifier(output_table))
            )

            # Insert selected labels with incrementing integer ids starting at 0
            for i, label in enumerate(labels_to_apply):
                cur.execute(
                    sql.SQL(
                        """
                        INSERT INTO {} (_id, label)
                        VALUES (%s, %s)
                        """
                    ).format(sql.Identifier(output_table)),
                    (i, label),
                )

        conn.commit()

    logger.info(
        f"Created {output_table} with {len(labels_to_apply)} Local Contexts label(s)."
    )

    return output_table