import logging

from f.common_logic.db_operations import conninfo, fetch_data_from_postgres, postgresql
from f.common_logic.save_disk import save_data_to_file

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(
    db: postgresql,
    db_table_name: str,
    storage_path: str = "/persistent-storage/datalake/export",
):
    columns, rows = fetch_data_from_postgres(conninfo(db), db_table_name)

    # Convert rows to lists to ensure compatibility with CSV writer, which requires iterable rows
    data = [columns, *map(list, rows)]

    save_data_to_file(data, db_table_name, storage_path, file_type="csv")
