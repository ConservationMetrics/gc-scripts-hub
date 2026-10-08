# py311

# extra_requirements:
# filetype==1.2.0
# fiona==1.10.1
# lxml==6.0.2
# openpyxl==3.1.5
# xlrd==2.0.2
# pandas==3.0.1

from f.common_logic.dataset_importer_v2 import ImportValidationError, stage_import


def main(
    db,
    uploaded_file,
    goal: str,
    target_table: str,
    replace_import_id: str | None = None,
):
    try:
        return stage_import(db, uploaded_file, goal, target_table, replace_import_id)
    except ImportValidationError as error:
        return {"validation_error": str(error)}
