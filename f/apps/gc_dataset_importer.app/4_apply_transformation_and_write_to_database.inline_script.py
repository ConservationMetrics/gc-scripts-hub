import json
from pathlib import Path

import pandas as pd

from f.common_logic.db_operations import postgresql
from f.common_logic.file_operations import save_data_to_file, save_uploaded_file_to_temp
from f.connectors.comapeo.comapeo_observations import transform_comapeo_observations
from f.connectors.geojson.geojson_to_postgres import main as save_geojson_to_postgres
from f.connectors.kobotoolbox.kobotoolbox_responses import (
    transform_kobotoolbox_form_data,
)

# from f.connectors.odk.odk_responses import transform_odk_form_data


def main(db: postgresql, uploaded_path, data_source, dataset_name, valid_sql_name):
    file_format = Path(uploaded_path).suffix.lower().lstrip(".")

    if file_format == "geojson":
        with open(uploaded_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if data_source == "comapeo":
            data = transform_comapeo_observations(data, dataset_name)
            transformed = True
        else:  # TODO: support locus_map, mapeo transformations
            transformed = False

        if transformed:
            output_filename = f"{Path(uploaded_path).stem}_transformed.geojson"
            saved = save_uploaded_file_to_temp(
                [{"name": output_filename, "data": json.dumps(data)}], is_base64=False
            )
            geojson_path = saved["file_paths"][0]
        else:
            geojson_path = uploaded_path

        save_geojson_to_postgres(
            db=db, db_table_name=valid_sql_name, geojson_path=geojson_path
        )

        save_data_to_file(
            data,
            dataset_name,
            f"/persistent-storage/datalake/{valid_sql_name}",
            file_type="geojson",
        )
        # TODO: Do we also want to save the original file?

    elif file_format == "csv":
        df = pd.read_csv(uploaded_path)

        if data_source == "kobotoolbox":
            df = transform_kobotoolbox_form_data(df, dataset_name)
            transformed = True
        # TODO: Fix ODK requirements issue where 'requests' imported by pyodk is
        # not compatible with the version of requests used by the rest of the codebase.
        # elif data_source == "odk":
        #     df = transform_odk_form_data(df, dataset_name)
        #    transformed = True
        else:
            transformed = False

        if transformed:
            output_filename = f"{Path(uploaded_path).stem}_transformed.csv"
            csv_str = df.to_csv(index=False)
            saved = save_uploaded_file_to_temp(
                [{"name": output_filename, "data": csv_str}], is_base64=False
            )
            csv_path = saved["file_paths"][0]
        else:
            csv_path = uploaded_path

        # TODO: There is no CSV to Postgres connector. What to do?
        # save_csv_to_postgres(db=db, db_table_name=dataset_name, csv_path=csv_path)

        save_data_to_file(
            df,
            dataset_name,
            f"/persistent-storage/datalake/{valid_sql_name}",
            file_type="csv",
        )
        # TODO: Do we also want to save the original file?
