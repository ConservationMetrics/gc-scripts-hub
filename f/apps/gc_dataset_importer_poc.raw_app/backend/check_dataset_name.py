from f.common_logic.dataset_importer_poc import check_dataset_name


def main(db, dataset_name: str):
    return check_dataset_name(db, dataset_name)
