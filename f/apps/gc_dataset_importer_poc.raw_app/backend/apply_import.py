from f.common_logic.dataset_importer_poc import apply_import


def main(db, import_id: str):
    return apply_import(db, import_id)
