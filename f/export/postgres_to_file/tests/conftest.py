import pytest
import testing.postgresql

import psycopg2


@pytest.fixture
def pg_database():
    """A dsn that may be used to connect to a live (local for test) postgresql server"""
    db = testing.postgresql.Postgresql(port=7654)
    dsn = db.dsn()
    dsn["dbname"] = dsn.pop("database")
    yield dsn
    db.stop()


@pytest.fixture
def database_mock_data(pg_database):
    """Fixture to insert mock data into the test database"""
    conn = psycopg2.connect(**pg_database)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE comapeo_data (
            _id TEXT,
            lat TEXT,
            deleted TEXT,
            created_at TEXT,
            updated_at TEXT,
            project_name TEXT,
            type TEXT,
            g__type TEXT,
            g__coordinates TEXT,
            attachments TEXT,
            notes TEXT,
            project_id TEXT,
            animal_type TEXT,
            lon TEXT
        );
    """)
    cursor.execute("""
        INSERT INTO comapeo_data VALUES
        ('doc_id_1', '-33.8688', 'False', '2024-10-14T20:18:14.206Z', '2024-10-14T20:18:14.206Z', 'Forest Expedition', 'water', 'Point', '[151.2093, -33.8688]', 'capybara.jpg', 'Rapid', 'forest_expedition', NULL, '151.2093'),
        ('doc_id_2', '48.8566', 'False', '2024-10-15T21:19:15.207Z', '2024-10-15T21:19:15.207Z', 'River Mapping', 'animal', 'Point', '[2.3522, 48.8566]', 'capybara.jpg', 'Capybara', 'river_mapping', 'capybara', '2.3522'),
        ('doc_id_3', 35.6895, 'False', '2024-10-16T22:20:16.208Z', '2024-10-16T22:20:16.208Z', 'Historical Site', 'location', 'Point', '[139.6917, 35.6895]', NULL, 'Former village site', 'historical', NULL, '139.6917');
    """)
    conn.commit()
    cursor.close()
    conn.close()
    yield pg_database
