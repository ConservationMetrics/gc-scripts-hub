from f.common_logic.dataset_importer_poc import preview_import


def main(db, import_id: str, identity_fields=None, update_policy: str = "imported"):
    return preview_import(db, import_id, identity_fields, update_policy)
