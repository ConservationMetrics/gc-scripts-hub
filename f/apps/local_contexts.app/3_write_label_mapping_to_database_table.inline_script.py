import logging

from f.common_logic.db_operations import (
    StructuredDBWriter,
    conninfo,
    postgresql,
    sql,
    truncate_table,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_LC_LABELS_SUFFIX = "lc_labels"


def _create_lc_labels_table(cursor, table_name: str):
    cursor.execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {table} (
                _id TEXT PRIMARY KEY,
                label TEXT NOT NULL
            )
            """
        ).format(table=sql.Identifier(table_name))
    )


def main(db: postgresql, table_name, labels_to_apply):
    """
    Create a Local Contexts label lookup table for a dataset.

    The output table is named `{table_name}__lc_labels` and contains:
    - `_id`: text primary key starting at "0"
    - `label`: Local Contexts label value

    If the table already exists, it is truncated and refilled.

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
    tuple[bool, str | None]
        A tuple containing (success, error_message):
        - success : bool
            True if the label table was refreshed successfully, False otherwise.
        - error_message : str or None
            Error message if success is False, None if success is True.
    """
    labels_to_apply = labels_to_apply or []

    try:
        writer = StructuredDBWriter(
            conninfo(db),
            table_name,
            suffix=_LC_LABELS_SUFFIX,
            predefined_schema=_create_lc_labels_table,
        )
        output_table = writer.table_name

        logger.info(f"Refreshing Local Contexts label table: {output_table}")
        truncate_table(conninfo(db), output_table)

        rows = [
            {"_id": str(i), "label": label}
            for i, label in enumerate(labels_to_apply)
        ]
        writer.handle_output(rows)

        logger.info(
            f"Created {output_table} with {len(labels_to_apply)} Local Contexts label(s)."
        )
        return True, None
    except Exception as e:
        error_msg = f"Error while refreshing Local Contexts label table: {e}"
        logger.error(error_msg)
        return False, error_msg