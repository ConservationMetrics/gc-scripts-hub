from f.common_logic.dataset_importer_poc import stage_import


def main(db, uploaded_file, goal: str, target_table: str):
    return stage_import(db, uploaded_file, goal, target_table)
