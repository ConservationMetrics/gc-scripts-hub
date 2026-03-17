from f.common_logic.db_operations import conninfo, postgresql, fetch_data_from_postgres

def main(db: postgresql, lcTableName):
    """
    Fetch Local Contexts labels from a Postgres table and split them into
    TK and BC label dictionaries.

    Assumes that `label_type` values are unique within the table.

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
        - "tk_labels": dict mapping {label_type: name} for TK labels
        - "bc_labels": dict mapping {label_type: name} for BC labels
    """
    if not lcTableName:
        return {
            "tk_labels": None,
            "bc_labels": None,
        }

    columns, rows = fetch_data_from_postgres(conninfo(db), lcTableName)

    label_type_idx = columns.index("label_type")
    name_idx = columns.index("name")
    label_category_idx = columns.index("label_category")

    tk_labels = {}
    bc_labels = {}

    for row in rows:
        label_type = row[label_type_idx]
        name = row[name_idx]
        category = row[label_category_idx]

        if category == "TK":
            tk_labels[label_type] = name
        elif category == "BC":
            bc_labels[label_type] = name

    return {
        "tk_labels": tk_labels,
        "bc_labels": bc_labels,
    }