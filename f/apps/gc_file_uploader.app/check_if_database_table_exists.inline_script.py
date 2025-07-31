from f.common_logic.db_operations import postgresql, conninfo
from f.common_logic.db_operations import check_if_table_exists

def main(db: postgresql, table_name: str):
    return check_if_table_exists(conninfo(db), table_name)