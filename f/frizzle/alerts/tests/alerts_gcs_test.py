from f.frizzle.alerts.alerts_gcs import main


def test_script(pg_database, tmp_path):
    asset_storage = tmp_path / "datalake"

    main(
        pg_database,
        asset_storage,
    )
