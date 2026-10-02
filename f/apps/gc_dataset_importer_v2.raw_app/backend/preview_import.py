from f.common_logic.dataset_importer_v2 import ImportValidationError, preview_import


def main(db, import_id: str, identity_fields=None, update_policy: str = "imported"):
    try:
        return preview_import(db, import_id, identity_fields, update_policy)
    except ImportValidationError as error:
        return {"validation_error": str(error)}
