from f.common_logic.dataset_importer_v2 import ImportValidationError, stage_import


def main(db, uploaded_file, goal: str, target_table: str):
    try:
        return stage_import(db, uploaded_file, goal, target_table)
    except ImportValidationError as error:
        return {"validation_error": str(error)}
