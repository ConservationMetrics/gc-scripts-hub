import logging

from f.common_logic.db_operations import conninfo, fetch_data_from_postgres, postgresql

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(db: postgresql, lcTableName):
    """
    Fetch Local Contexts labels from a Postgres table and split them into
    TK and BC label option lists for UI consumption.

    Each option is formatted as:
    { "label": label_type, "value": name }

    Parameters
    ----------
    db : postgresql
        Database connection resource used to access the Postgres instance.
    lcTableName : str
        Name of the table containing Local Contexts label data.

    Returns
    -------
    dict
        Dictionary with two keys:
        - "tk_labels": list of option objects for TK labels, or None
        - "bc_labels": list of option objects for BC labels, or None
    """

    if not lcTableName:
        return {
            "tk_labels": None,
            "bc_labels": None,
        }

    logger.info(f"Fetching Local Contexts labels from table: {lcTableName}")

    columns, rows = fetch_data_from_postgres(conninfo(db), lcTableName)

    label_type_idx = columns.index("label_type")
    name_idx = columns.index("name")
    label_category_idx = columns.index("label_category")

    tk_labels = []
    bc_labels = []

    for row in rows:
        label_type = row[label_type_idx]
        name = row[name_idx]
        category = row[label_category_idx]

        option = {
            "label": name,
            "value": label_type,
        }

        if category == "TK":
            tk_labels.append(option)
        elif category == "BC":
            bc_labels.append(option)

    logger.info(f"Fetched {len(tk_labels)} TK labels and {len(bc_labels)} BC labels")

    return {
        "tk_labels": tk_labels,
        "bc_labels": bc_labels,
    }
