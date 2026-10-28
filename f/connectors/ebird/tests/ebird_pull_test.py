import json
from urllib.parse import parse_qs, urlparse

import psycopg
import pytest
import requests

from f.common_logic.db_operations import USE_EXISTING_DATASET
from f.connectors.ebird.ebird_pull import (
    download_observations,
    main,
    parse_region_code,
    transform_observations_to_geojson,
)
from f.connectors.ebird.tests.assets.server_responses import (
    HYBRID_FEATURE_ID,
    NOTABLE_COUNT,
    NOTABLE_OBS_ID,
    NOTABLE_OBSERVATIONS,
    NOTABLE_SECOND_HERON_ID,
    NOTABLE_SPARROW_ID,
    PRIMARY_FEATURE_ID,
    PUBLIC_LOCATION_FEATURE_ID,
    RECENT_COUNT,
    RECENT_OBSERVATIONS,
)

API_TOKEN = "test-token"
REGION_CODE = "US-NY"
LON_LAT = "79.455132, 43.530936"
SEARCH_LON = 79.455132
SEARCH_LAT = 43.530936


def test_fixture_counts():
    assert len(RECENT_OBSERVATIONS) == RECENT_COUNT
    assert len(NOTABLE_OBSERVATIONS) == NOTABLE_COUNT


def _pull(pg_database, tmp_path, table_name, **kwargs):
    storage = tmp_path / "datalake"
    main(
        api_token=API_TOKEN,
        db=pg_database,
        db_table_name=table_name,
        attachment_root=storage,
        **kwargs,
    )
    return storage


def _request(mocked_responses):
    assert len(mocked_responses.calls) == 1
    call = mocked_responses.calls[0]
    parsed = urlparse(call.request.url)
    params = {key: values[0] for key, values in parse_qs(parsed.query).items()}
    assert call.request.headers["X-eBirdApiToken"] == API_TOKEN
    return parsed.path, params


def _row(cur, table_name, feature_id, columns):
    cur.execute(
        f"SELECT {', '.join(columns)} FROM {table_name} WHERE _id = %s",
        (feature_id,),
    )
    values = cur.fetchone()
    assert values is not None
    return dict(zip(columns, values))


def _columns(cur, table_name):
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        """,
        (table_name,),
    )
    return {row[0] for row in cur.fetchall()}


def _truthy(value) -> bool:
    return str(value).lower() in {"true", "t", "1"}


def test_recent_region_e2e(ebird_recent, mocked_responses, pg_database, tmp_path):
    table_name = "ebird_recent_region"
    storage = _pull(
        pg_database,
        tmp_path,
        table_name,
        observation_type="recent_region",
        region_code=f"  {REGION_CODE}  ",
        lon_lat="not-a-coordinate",
    )

    path, params = _request(mocked_responses)
    assert path == f"/v2/data/obs/{REGION_CODE}/recent"
    assert params == {}

    raw_path = storage / table_name / f"{table_name}_observations.json"
    geojson_path = storage / table_name / f"{table_name}.geojson"
    assert json.loads(raw_path.read_text()) == RECENT_OBSERVATIONS
    geojson = json.loads(geojson_path.read_text())
    assert geojson["type"] == "FeatureCollection"
    assert len(geojson["features"]) == RECENT_COUNT
    assert API_TOKEN not in raw_path.read_text()
    assert API_TOKEN not in geojson_path.read_text()

    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            assert cur.fetchone()[0] == RECENT_COUNT

            columns = _columns(cur, table_name)
            assert "region_code" in columns
            assert "search_lon" not in columns

            primary = _row(
                cur,
                table_name,
                PRIMARY_FEATURE_ID,
                [
                    "g__type",
                    "g__coordinates",
                    "data_source",
                    "observation_type",
                    "common_name",
                    "scientific_name",
                    "loc_name",
                    "observed_at",
                    "how_many",
                    "location_private",
                    "region_code",
                    "user_display_name",
                    "obs_id",
                ],
            )
            assert primary["g__type"] == "Point"
            assert primary["g__coordinates"] == "[79.455132, 43.530936]"
            assert primary["data_source"] == "eBird"
            assert primary["observation_type"] == "recent_region"
            assert primary["common_name"] == "Hooded Crow"
            assert primary["scientific_name"] == "Corvus cornix"
            assert primary["loc_name"].startswith("улица Старикова")
            assert primary["observed_at"] == "2020-01-21 16:35"
            assert str(primary["how_many"]) in {"1", "1.0"}
            assert _truthy(primary["location_private"])
            assert primary["region_code"] == REGION_CODE
            assert primary["user_display_name"] is None
            assert primary["obs_id"] is None

            public = _row(
                cur,
                table_name,
                PUBLIC_LOCATION_FEATURE_ID,
                ["location_private", "scientific_name"],
            )
            assert not _truthy(public["location_private"])
            assert public["scientific_name"] == "Passer montanus"

            hybrid = _row(cur, table_name, HYBRID_FEATURE_ID, ["common_name"])
            assert hybrid["common_name"] == "House/Eurasian Tree Sparrow"


def test_notable_region_e2e(ebird_notable, mocked_responses, pg_database, tmp_path):
    table_name = "ebird_notable_region"
    _pull(
        pg_database,
        tmp_path,
        table_name,
        observation_type="recent_notable_region",
        region_code=REGION_CODE,
    )

    path, params = _request(mocked_responses)
    assert path == f"/v2/data/obs/{REGION_CODE}/recent/notable"
    assert params == {"detail": "full"}

    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            assert cur.fetchone()[0] == NOTABLE_COUNT

            cur.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE species_code = %s",
                ("libher",),
            )
            assert cur.fetchone()[0] == 2

            heron = _row(
                cur,
                table_name,
                NOTABLE_OBS_ID,
                [
                    "g__coordinates",
                    "common_name",
                    "scientific_name",
                    "user_display_name",
                    "observer_first_name",
                    "observer_last_name",
                    "country_name",
                    "subnational1_code",
                    "checklist_id",
                    "obs_valid",
                    "has_rich_media",
                    "observation_type",
                ],
            )
            assert heron["g__coordinates"] == "[-74.5262526, 41.2833266]"
            assert heron["common_name"] == "Little Blue Heron"
            assert heron["scientific_name"] == "Egretta caerulea"
            assert heron["user_display_name"] == "Kathleen Ashman"
            assert heron["observer_first_name"] == "Kathleen"
            assert heron["observer_last_name"] == "Astahman"
            assert heron["country_name"] == "United States"
            assert heron["subnational1_code"] == "US-NY"
            assert heron["checklist_id"] == "CL22364"
            assert str(heron["obs_valid"]).lower() in {"false", "f", "0"}
            assert str(heron["has_rich_media"]).lower() in {"false", "f", "0"}
            assert heron["observation_type"] == "recent_notable_region"

            assert (
                _row(cur, table_name, NOTABLE_SECOND_HERON_ID, ["obs_id"])["obs_id"]
                == NOTABLE_SECOND_HERON_ID
            )


def test_recent_nearby_e2e(ebird_recent, mocked_responses, pg_database, tmp_path):
    table_name = "ebird_recent_nearby"
    _pull(
        pg_database,
        tmp_path,
        table_name,
        observation_type="recent_nearby",
        lon_lat=f"  {LON_LAT}  ",
        region_code="not a region",
    )

    path, params = _request(mocked_responses)
    assert path == "/v2/data/obs/geo/recent"
    assert float(params["lng"]) == SEARCH_LON
    assert float(params["lat"]) == SEARCH_LAT
    assert "detail" not in params

    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            assert cur.fetchone()[0] == RECENT_COUNT
            columns = _columns(cur, table_name)
            assert "search_lon" in columns
            assert "search_lat" in columns
            assert "region_code" not in columns
            row = _row(
                cur,
                table_name,
                PRIMARY_FEATURE_ID,
                ["search_lon", "search_lat", "observation_type"],
            )
            assert float(row["search_lon"]) == SEARCH_LON
            assert float(row["search_lat"]) == SEARCH_LAT
            assert row["observation_type"] == "recent_nearby"


def test_notable_nearby_e2e(ebird_notable, mocked_responses, pg_database, tmp_path):
    table_name = "ebird_notable_nearby"
    _pull(
        pg_database,
        tmp_path,
        table_name,
        observation_type="recent_notable_nearby",
        lon_lat="-74.5262526, 41.2833266",
    )

    path, params = _request(mocked_responses)
    assert path == "/v2/data/obs/geo/recent/notable"
    assert params["detail"] == "full"
    assert float(params["lng"]) == -74.5262526
    assert float(params["lat"]) == 41.2833266

    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            assert cur.fetchone()[0] == NOTABLE_COUNT
            row = _row(
                cur,
                table_name,
                NOTABLE_SPARROW_ID,
                ["common_name", "search_lon", "search_lat"],
            )
            assert row["common_name"] == "Lark Sparrow"
            assert float(row["search_lon"]) == -74.5262526
            assert float(row["search_lat"]) == 41.2833266


def test_historic_date_e2e(ebird_recent, mocked_responses, pg_database, tmp_path):
    table_name = "ebird_historic"
    _pull(
        pg_database,
        tmp_path,
        table_name,
        observation_type="historic_date",
        region_code=REGION_CODE,
        observation_date="2020-01-21",
        lon_lat="not-a-coordinate",
    )

    path, params = _request(mocked_responses)
    assert path == f"/v2/data/obs/{REGION_CODE}/historic/2020/1/21"
    assert params == {}

    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            assert cur.fetchone()[0] == RECENT_COUNT
            columns = _columns(cur, table_name)
            assert "region_code" in columns
            assert "observation_date" in columns
            assert "search_lon" not in columns
            row = _row(
                cur,
                table_name,
                PRIMARY_FEATURE_ID,
                ["region_code", "observation_date", "observation_type", "observed_at"],
            )
            assert row["region_code"] == REGION_CODE
            assert row["observation_date"] == "2020-01-21"
            assert row["observation_type"] == "historic_date"
            assert row["observed_at"] == "2020-01-21 16:35"


def test_empty_response_skips_database(ebird_empty, pg_database, tmp_path):
    table_name = "ebird_empty"
    storage = _pull(
        pg_database,
        tmp_path,
        table_name,
        observation_type="recent_region",
        region_code=REGION_CODE,
    )

    assert not (storage / table_name / f"{table_name}.geojson").exists()
    assert not (storage / table_name / f"{table_name}_observations.json").exists()
    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s)", (f"public.{table_name}",))
            assert cur.fetchone()[0] is None


def test_use_existing_dataset_requires_a_real_table(pg_database):
    with pytest.raises(ValueError, match="does not exist"):
        main(
            observation_type="recent_region",
            api_token=API_TOKEN,
            db=pg_database,
            region_code=REGION_CODE,
            destination_action=USE_EXISTING_DATASET,
            existing_db_table_name="missing_dataset",
        )


def test_use_existing_dataset_writes_to_that_table(
    ebird_recent, pg_database, tmp_path
):
    table_name = "ebird_existing"
    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE {table_name} (_id text PRIMARY KEY)")

    _pull(
        pg_database,
        tmp_path,
        table_name,
        observation_type="recent_region",
        region_code=REGION_CODE,
        destination_action=USE_EXISTING_DATASET,
        existing_db_table_name=table_name,
    )

    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            assert cur.fetchone()[0] == RECENT_COUNT


def test_download_observations_rejects_non_list(mocked_responses):
    mocked_responses.get(
        "https://api.ebird.org/v2/data/obs/US-NY/recent",
        json={"errors": [{"title": "nope"}]},
        status=200,
    )
    with pytest.raises(ValueError, match="JSON list"):
        download_observations(
            API_TOKEN, "https://api.ebird.org/v2/data/obs/US-NY/recent", {}
        )


def test_download_observations_raises_for_http_error(mocked_responses):
    mocked_responses.get(
        "https://api.ebird.org/v2/data/obs/US-NY/recent",
        json={"errors": [{"title": "forbidden"}]},
        status=403,
    )
    with pytest.raises(requests.HTTPError):
        download_observations(
            API_TOKEN, "https://api.ebird.org/v2/data/obs/US-NY/recent", {}
        )
    assert (
        mocked_responses.calls[0].request.headers["X-eBirdApiToken"] == API_TOKEN
    )


def test_duplicate_observation_id():
    row = RECENT_OBSERVATIONS[0]
    with pytest.raises(ValueError, match="Duplicate observation id"):
        transform_observations_to_geojson(
            [row, dict(row)],
            observation_type="recent_region",
        )


def test_transform_missing_coordinates():
    row = dict(RECENT_OBSERVATIONS[0])
    row["lat"] = None
    row["lng"] = None
    feature = transform_observations_to_geojson(
        [row], observation_type="recent_region"
    )["features"][0]
    assert feature["geometry"] is None
    assert feature["id"] == PRIMARY_FEATURE_ID


def test_transform_missing_identity():
    with pytest.raises(ValueError, match="obsId"):
        transform_observations_to_geojson(
            [{"comName": "Unknown"}],
            observation_type="recent_region",
        )


def test_notable_feature_id_prefers_obs_id():
    feature = transform_observations_to_geojson(
        NOTABLE_OBSERVATIONS[:1],
        observation_type="recent_notable_region",
        region_code=REGION_CODE,
    )["features"][0]
    assert feature["id"] == NOTABLE_OBS_ID
    assert feature["properties"]["obs_id"] == NOTABLE_OBS_ID
    assert "locID" not in feature["properties"]


def test_api_token_required():
    with pytest.raises(ValueError, match="api_token is required"):
        main(
            observation_type="recent_region",
            api_token="   ",
            db={},
            region_code=REGION_CODE,
            db_table_name="ebird_token",
        )


def test_invalid_observation_type():
    with pytest.raises(ValueError, match="Invalid observation_type"):
        main(
            observation_type="historic",
            api_token=API_TOKEN,
            db={},
            region_code=REGION_CODE,
            db_table_name="ebird_type",
        )


def test_region_query_requires_region_code():
    with pytest.raises(ValueError, match="region_code"):
        main(
            observation_type="recent_notable_region",
            api_token=API_TOKEN,
            db={},
            region_code="",
            db_table_name="ebird_region",
        )


@pytest.mark.parametrize("region_code", ["US NY", "US-", "../secret", "US/NY"])
def test_invalid_region_code(region_code):
    with pytest.raises(ValueError, match="region_code"):
        parse_region_code(region_code)


@pytest.mark.parametrize("region_code", ["US", "US-NY", "US-NY-071", "L295658"])
def test_valid_region_code(region_code):
    assert parse_region_code(f"  {region_code}  ") == region_code


def test_nearby_query_requires_lon_lat():
    with pytest.raises(ValueError, match="lon_lat"):
        main(
            observation_type="recent_nearby",
            api_token=API_TOKEN,
            db={},
            db_table_name="ebird_nearby",
        )


def test_main_rejects_longitude_out_of_range():
    with pytest.raises(ValueError, match="longitude must be between -180 and 180"):
        main(
            observation_type="recent_notable_nearby",
            api_token=API_TOKEN,
            db={},
            lon_lat="181, 41.2",
            db_table_name="ebird_bounds",
        )


def test_historic_requires_region_and_date():
    with pytest.raises(ValueError, match="region_code"):
        main(
            observation_type="historic_date",
            api_token=API_TOKEN,
            db={},
            region_code="",
            observation_date="2020-01-21",
            db_table_name="ebird_historic_region",
        )
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        main(
            observation_type="historic_date",
            api_token=API_TOKEN,
            db={},
            region_code=REGION_CODE,
            db_table_name="ebird_historic_date",
        )


def test_historic_rejects_dates_before_1800():
    with pytest.raises(ValueError, match="on or after 1800-01-01"):
        main(
            observation_type="historic_date",
            api_token=API_TOKEN,
            db={},
            region_code=REGION_CODE,
            observation_date="1799-12-31",
            db_table_name="ebird_historic_early",
        )
