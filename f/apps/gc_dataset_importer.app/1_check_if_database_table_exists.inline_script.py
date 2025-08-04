from f.common_logic.db_operations import check_if_table_exists, conninfo, postgresql
from f.common_logic.db_transformations import force_valid_sql_name


def main(db: postgresql, dataset_name: str):
    valid_sql_name = force_valid_sql_name(dataset_name)

    table_exists = check_if_table_exists(conninfo(db), valid_sql_name)

    return {
        "tableExists": table_exists,
        "datasetName": dataset_name,
        "validSqlName": valid_sql_name,
    }
