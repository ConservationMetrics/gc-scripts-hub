import logging

from f.common_logic.db_operations import (
    check_if_table_exists,
    conninfo,
    fetch_data_from_postgres,
    postgresql,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _build_label_options(columns, rows):
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

    return tk_labels, bc_labels


def _split_applied_labels_by_category(columns, rows, applied_label_types):
    label_type_idx = columns.index("label_type")
    label_category_idx = columns.index("label_category")

    tk_labels_already_applied = []
    bc_labels_already_applied = []

    for row in rows:
        label_type = row[label_type_idx]
        category = row[label_category_idx]

        if label_type not in applied_label_types:
            continue

        if category == "TK":
            tk_labels_already_applied.append(label_type)
        elif category == "BC":
            bc_labels_already_applied.append(label_type)

    return tk_labels_already_applied, bc_labels_already_applied


def _get_labels_already_applied(db: postgresql, dataset_name: str | None):
    if not dataset_name:
        return None

    mapping_table = f"{dataset_name}__lc_labels"
    db_connection_string = conninfo(db)

    if not check_if_table_exists(db_connection_string, mapping_table):
        logger.info(
            f"No existing Local Contexts label mapping table found: {mapping_table}"
        )
        return []

    columns, rows = fetch_data_from_postgres(db_connection_string, mapping_table)
    if "label" not in columns:
        logger.warning(
            f"Local Contexts label mapping table {mapping_table} is missing 'label' column"
        )
        return []

    label_idx = columns.index("label")
    return {row[label_idx] for row in rows}


def main(db: postgresql, lcTableName, dataset_name):
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
        Name of the table containing Local Contexts label definitions.
    dataset_name : str
        Base dataset table name used to locate existing label mappings.

    Returns
    -------
    dict
        Dictionary with four keys:
        - "tk_labels": list of option objects for TK labels, or None
        - "bc_labels": list of option objects for BC labels, or None
        - "tk_labels_already_applied": list of TK label_type values already mapped
        - "bc_labels_already_applied": list of BC label_type values already mapped
    """

    if not lcTableName:
        return {
            "tk_labels_available": None,
            "bc_labels_available": None,
            "tk_labels_already_applied": None,
            "bc_labels_already_applied": None,
        }

    logger.info(f"Fetching Local Contexts labels from table: {lcTableName}")

    columns, rows = fetch_data_from_postgres(conninfo(db), lcTableName)
    tk_labels, bc_labels = _build_label_options(columns, rows)

    logger.info(f"Fetched {len(tk_labels)} TK labels and {len(bc_labels)} BC labels")

    applied_label_types = _get_labels_already_applied(db, dataset_name)
    tk_labels_already_applied, bc_labels_already_applied = (
        _split_applied_labels_by_category(columns, rows, applied_label_types)
        if applied_label_types
        else ([], [])
    )

    return {
        "tk_labels_available": tk_labels,
        "bc_labels_available": bc_labels,
        "tk_labels_already_applied": tk_labels_already_applied,
        "bc_labels_already_applied": bc_labels_already_applied,
    }
