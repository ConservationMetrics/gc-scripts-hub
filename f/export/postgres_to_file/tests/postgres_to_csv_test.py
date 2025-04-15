from f.export.postgres_to_file.postgres_to_csv import main


def test_script_e2e(pg_database, database_mock_data, tmp_path):
    asset_storage = tmp_path / "datalake/export"

    main(
        pg_database,
        "comapeo_data",
        asset_storage,
    )

    with open(asset_storage / "comapeo_data.csv") as f:
        data = f.read()
        assert data == (
            '"_id","lat","deleted","created_at","updated_at","project_name",'
            '"type","g__type","g__coordinates","attachments","notes",'
            '"project_id","animal_type","lon"\n'
            '"doc_id_1","-33.8688","False","2024-10-14T20:18:14.206Z",'
            '"2024-10-14T20:18:14.206Z","Forest Expedition","water",'
            '"Point","[151.2093, -33.8688]","capybara.jpg","Rapid",'
            '"forest_expedition","","151.2093"\n'
            '"doc_id_2","48.8566","False","2024-10-15T21:19:15.207Z",'
            '"2024-10-15T21:19:15.207Z","River Mapping","animal",'
            '"Point","[2.3522, 48.8566]","capybara.jpg","Capybara",'
            '"river_mapping","capybara","2.3522"\n'
            '"doc_id_3","35.6895","False","2024-10-16T22:20:16.208Z",'
            '"2024-10-16T22:20:16.208Z","Historical Site","location",'
            '"Point","[139.6917, 35.6895]","","Former village site",'
            '"historical","","139.6917"\n'
        )
