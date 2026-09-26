from f.common_logic.dataset_importer_v2 import apply_import


def main(db, import_id: str, preview_id: str):
    return apply_import(db, import_id, preview_id)
