from f.common_logic.db_operations import conninfo, postgresql, fetch_tables_from_postgres

def main(db: postgresql):
    return fetch_tables_from_postgres(conninfo(db))