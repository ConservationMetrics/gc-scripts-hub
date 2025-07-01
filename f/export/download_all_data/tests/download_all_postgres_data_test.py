import csv
import zipfile
from pathlib import Path

import psycopg2

from f.common_logic.db_operations import postgresql
from f.export.download_all_data.download_all_postgres_data import main


def test_download_all_data(pg_database):
    conn = psycopg2.connect(**pg_database)
    cur = conn.cursor()

    cur.execute("CREATE TABLE apples (id SERIAL PRIMARY KEY, name TEXT)")
    cur.execute("INSERT INTO apples (name) VALUES ('fuji'), ('gala'), ('granny smith')")

    cur.execute("CREATE TABLE bananas (id SERIAL PRIMARY KEY, ripeness TEXT)")
    cur.execute(
        "INSERT INTO bananas (ripeness) VALUES ('green'), ('yellow'), ('brown'), ('overripe')"
    )

    cur.execute("""
        CREATE TABLE oranges (
            id SERIAL PRIMARY KEY,
            color TEXT,
            variety TEXT,
            weight DECIMAL,
            ripe BOOLEAN,
            harvested_at TIMESTAMP
        )
    """)

    cur.execute("""
        INSERT INTO oranges (color, variety, weight, ripe, harvested_at)
        VALUES 
            ('orange', 'valencia', 0.28, TRUE, '2024-11-01 08:30:00'),
            ('green', 'navel', 0.35, FALSE, NULL),
            ('orange', 'blood', 0.30, TRUE, '2024-10-15 14:00:00'),
            ('yellow', 'cara cara', 0.32, TRUE, '2024-09-10 07:45:00')
    """)

    conn.commit()
    cur.close()
    conn.close()

    db = postgresql(pg_database)
    output_path = Path("/tmp/test-csv-exports")
    output_path.mkdir(parents=True, exist_ok=True)

    main(db=db, storage_path=str(output_path))

    zip_path = output_path / "all_database_content.zip"
    assert zip_path.exists()

    with zipfile.ZipFile(zip_path, "r") as zipf:
        names = zipf.namelist()
        assert set(names) == {"apples.csv", "bananas.csv", "oranges.csv"}

        def read_csv_from_zip(name):
            with zipf.open(name) as f:
                return list(csv.reader(line.decode("utf-8") for line in f))

        assert read_csv_from_zip("apples.csv") == [
            ["id", "name"],
            ["1", "fuji"],
            ["2", "gala"],
            ["3", "granny smith"],
        ]

        assert read_csv_from_zip("bananas.csv") == [
            ["id", "ripeness"],
            ["1", "green"],
            ["2", "yellow"],
            ["3", "brown"],
            ["4", "overripe"],
        ]

        assert read_csv_from_zip("oranges.csv") == [
            ["id", "color", "variety", "weight", "ripe", "harvested_at"],
            ["1", "orange", "valencia", "0.28", "t", "2024-11-01 08:30:00"],
            ["2", "green", "navel", "0.35", "f", ""],
            ["3", "orange", "blood", "0.30", "t", "2024-10-15 14:00:00"],
            ["4", "yellow", "cara cara", "0.32", "t", "2024-09-10 07:45:00"],
        ]
