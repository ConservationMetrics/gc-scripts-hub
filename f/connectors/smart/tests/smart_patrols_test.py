from pathlib import Path

import psycopg2
import pytest

from f.connectors.smart.smart_patrols import main, parse_smart_patrol_xml


def test_parse_smart_patrol_xml():
    """Test parsing SMART patrol XML file."""
    xml_path = Path("f/connectors/smart/tests/assets/SMART_000006_001.xml")
    
    geojson = parse_smart_patrol_xml(xml_path)
    
    # Check that we got a valid GeoJSON FeatureCollection
    assert geojson["type"] == "FeatureCollection"
    assert "features" in geojson
    
    features = geojson["features"]
    
    # Waypoint 2 (id="2") has 8 observations
    # Waypoint 10 (id="10") has 3 observations
    # Total: 11 observations
    assert len(features) == 11
    
    # Check first observation (from waypoint 2)
    first_feature = features[0]
    assert first_feature["type"] == "Feature"
    assert "id" in first_feature
    assert "properties" in first_feature
    assert "geometry" in first_feature
    
    first_obs = first_feature["properties"]
    
    # Patrol-level fields
    assert first_obs["patrol_id"] == "SMART_000006-001"
    assert first_obs["patrol_type"] == "GROUND"
    assert first_obs["patrol_start_date"] == "2025-12-17"
    assert first_obs["patrol_end_date"] == "2026-01-31"
    assert first_obs["patrol_is_armed"] == "false"
    assert first_obs["patrol_description"] == "Test project"
    assert first_obs["patrol_team"] == "Community Team 1"
    assert first_obs["patrol_comment"] == "Test patrol for monitoring project"
    
    # Leg-level fields
    assert first_obs["leg_id"] == "1"
    assert first_obs["leg_start_date"] == "2025-12-17"
    assert first_obs["leg_end_date"] == "2025-12-17"
    assert first_obs["leg_transport_type"] == "Vehicle"
    assert "John Peter" in first_obs["leg_members"]
    assert "Tony Wambu" in first_obs["leg_members"]
    assert "Tim Obriek" in first_obs["leg_members"]
    assert "David Aliata" in first_obs["leg_members"]
    assert "Lilian Wendy" in first_obs["leg_members"]
    assert first_obs["leg_mandate"] == "Research and Monitoring"
    
    # Day-level fields
    assert first_obs["day_date"] == "2025-12-17"
    assert first_obs["day_start_time"] == "20:48:25"
    assert first_obs["day_end_time"] == "20:51:55"
    assert first_obs["day_rest_minutes"] == "2.0"
    
    # Waypoint-level fields
    assert first_obs["waypoint_id"] == "2"
    assert first_obs["waypoint_x"] == "-77.19774596"
    assert first_obs["waypoint_y"] == "38.7606911"
    assert first_obs["waypoint_time"] == "20:50:41"
    
    # Observation category
    assert first_obs["category"] == "humanactivity.timber.firewood."
    
    # Geometry fields
    assert first_feature["geometry"]["type"] == "Point"
    assert first_feature["geometry"]["coordinates"] == [-77.19774596, 38.7606911]
    
    # Check that attributes are extracted correctly
    # Find the firewood observation
    firewood_feature = next(
        f for f in features 
        if f["properties"]["category"] == "humanactivity.timber.firewood."
    )
    firewood_obs = firewood_feature["properties"]
    assert firewood_obs["threat"] == "residentialcommercialdevelopment.commericalindustrialareas."
    assert firewood_obs["treespecies_timber"] == "rosewood"
    assert firewood_obs["actiontakenitems"] == "confiscated"
    assert firewood_obs["numberofbundles"] == 64.0
    assert firewood_obs["ageofsign"] == "veryold"
    
    # Find the waterhole observation
    waterhole_feature = next(
        f for f in features 
        if f["properties"]["category"] == "features.waterhole."
    )
    waterhole_obs = waterhole_feature["properties"]
    assert waterhole_obs["haswater"] is True
    assert waterhole_obs["species"] == "chordata_rl.mammalia_rl.cetartiodactyla_rl.hippopotamidae_rl.hippopotamus_rl.hippopotamusamphibius_rl10103."
    
    # Find the people observation
    people_feature = next(
        f for f in features 
        if f["properties"]["category"] == "humanactivity.people."
    )
    people_obs = people_feature["properties"]
    assert people_obs["sex"] == "unknown"
    assert people_obs["phonenumber"] == "231959932"
    assert people_obs["peoplearmed"] == "unarmed"
    assert people_obs["numberofpeople"] == 50.0
    assert people_obs["nationalidnumber"] == "H838582"
    assert people_obs["nameornames"] == "James, Fred, ..."
    assert people_obs["personage"] == 90.0
    
    # Check second waypoint observations
    waypoint_10_features = [
        f for f in features 
        if f["properties"]["waypoint_id"] == "10"
    ]
    assert len(waypoint_10_features) == 3
    
    # Check position observation from waypoint 10
    position_feature = next(
        f for f in waypoint_10_features 
        if f["properties"]["category"] == "position."
    )
    position_obs = position_feature["properties"]
    assert position_obs["waypoint_id"] == "10"
    assert position_obs["waypoint_x"] == "-77.19774609"
    assert position_obs["waypoint_y"] == "38.76069271"
    assert position_obs["waypoint_time"] == "20:51:38"
    assert position_obs["positiontype"] == "start"
    assert position_feature["geometry"]["type"] == "Point"
    assert position_feature["geometry"]["coordinates"] == [-77.19774609, 38.76069271]


def test_script_e2e(pg_database, tmp_path):
    """Test the full script execution."""
    asset_storage = tmp_path / "datalake"
    asset_storage.mkdir(parents=True)
    
    # Copy test XML to the asset storage
    xml_source = Path("f/connectors/smart/tests/assets/SMART_000006_001.xml")
    xml_dest = asset_storage / "SMART_000006_001.xml"
    xml_dest.write_text(xml_source.read_text())
    
    # Run the main script
    main(
        smart_patrols_path="SMART_000006_001.xml",
        db=pg_database,
        db_table_name="smart_patrol_test",
        attachment_root=str(asset_storage),
    )
    
    # Verify XML was saved to project folder
    saved_xml = asset_storage / "smart_patrol_test" / "SMART_000006_001.xml"
    assert saved_xml.exists()
    
    # Verify GeoJSON was saved to project folder
    saved_geojson = asset_storage / "smart_patrol_test" / "smart_patrol_test.geojson"
    assert saved_geojson.exists()
    
    # Verify database table was created and populated
    with psycopg2.connect(**pg_database) as conn:
        with conn.cursor() as cursor:
            # Check that table exists and has correct number of rows
            cursor.execute("SELECT COUNT(*) FROM smart_patrol_test")
            assert cursor.fetchone()[0] == 11
            
            # Get column names
            cursor.execute("SELECT * FROM smart_patrol_test LIMIT 0")
            columns = [desc[0] for desc in cursor.description]
            
            # Verify all expected columns are present
            expected_columns = [
                "patrol_id",
                "patrol_type",
                "patrol_start_date",
                "patrol_end_date",
                "patrol_is_armed",
                "patrol_description",
                "patrol_team",
                "patrol_comment",
                "leg_id",
                "leg_start_date",
                "leg_end_date",
                "leg_transport_type",
                "leg_members",
                "leg_mandate",
                "day_date",
                "day_start_time",
                "day_end_time",
                "day_rest_minutes",
                "waypoint_id",
                "waypoint_x",
                "waypoint_y",
                "waypoint_time",
                "category",
                "g__type",
                "g__coordinates",
                "data_source",
            ]
            for col in expected_columns:
                assert col in columns, f"Column {col} not found in table"
            
            # Check specific observation data
            cursor.execute(
                "SELECT patrol_id, patrol_team, leg_members, waypoint_id, category "
                "FROM smart_patrol_test WHERE category = 'humanactivity.timber.firewood.'"
            )
            row = cursor.fetchone()
            assert row[0] == "SMART_000006-001"
            assert row[1] == "Community Team 1"
            assert "John Peter" in row[2]
            assert "Tony Wambu" in row[2]
            assert row[3] == "2"
            assert row[4] == "humanactivity.timber.firewood."
            
            # Check attribute columns
            cursor.execute(
                "SELECT threat, treespecies_timber, actiontakenitems, numberofbundles, ageofsign "
                "FROM smart_patrol_test WHERE category = 'humanactivity.timber.firewood.'"
            )
            row = cursor.fetchone()
            assert row[0] == "residentialcommercialdevelopment.commericalindustrialareas."
            assert row[1] == "rosewood"
            assert row[2] == "confiscated"
            assert row[3] == "64.0"  # Stored as text in database
            assert row[4] == "veryold"
            
            # Check boolean value
            cursor.execute(
                "SELECT haswater FROM smart_patrol_test WHERE category = 'features.waterhole.'"
            )
            row = cursor.fetchone()
            assert row[0] == "true"  # Stored as lowercase text in database
            
            # Check string value
            cursor.execute(
                "SELECT phonenumber, nationalidnumber, nameornames "
                "FROM smart_patrol_test WHERE category = 'humanactivity.people.'"
            )
            row = cursor.fetchone()
            assert row[0] == "231959932"
            assert row[1] == "H838582"
            assert row[2] == "James, Fred, ..."
            
            # Check geometry fields
            cursor.execute(
                "SELECT g__type, g__coordinates FROM smart_patrol_test WHERE category = 'humanactivity.timber.firewood.'"
            )
            row = cursor.fetchone()
            assert row[0] == "Point"
            assert row[1] == "[-77.19774596, 38.7606911]"  # Stored as JSON string
            
            # Verify waypoint coordinates
            cursor.execute(
                "SELECT DISTINCT waypoint_x, waypoint_y FROM smart_patrol_test ORDER BY waypoint_x"
            )
            waypoints = cursor.fetchall()
            assert len(waypoints) == 2
            # Ordering is by string, so waypoint 2 comes before waypoint 10
            assert waypoints[0] == ("-77.19774596", "38.7606911")   # waypoint 2
            assert waypoints[1] == ("-77.19774609", "38.76069271")  # waypoint 10


def test_missing_xml_file(pg_database, tmp_path):
    """Test that script raises error when XML file doesn't exist."""
    asset_storage = tmp_path / "datalake"
    asset_storage.mkdir(parents=True)
    
    with pytest.raises(FileNotFoundError):
        main(
            smart_patrols_path="nonexistent.xml",
            db=pg_database,
            db_table_name="smart_patrol_test",
            attachment_root=str(asset_storage),
        )

