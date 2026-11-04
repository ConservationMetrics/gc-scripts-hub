from datetime import date, datetime, timedelta
from unittest.mock import patch

import pytest

from f.common_logic.date_utils import calculate_cutoff_date, parse_observation_date


@patch("f.common_logic.date_utils.datetime")
def test_calculate_cutoff_date_with_lookback(mock_datetime):
    """Test calculate_cutoff_date with a lookback period"""
    # Mock current date to October 2025
    mock_datetime.now.return_value = datetime(2025, 10, 15)

    # 6 months back from October 2025 should be April 2025
    result = calculate_cutoff_date(max_months_lookback=6)
    assert result == (2025, 4)

    # 12 months back should be October 2024
    result = calculate_cutoff_date(max_months_lookback=12)
    assert result == (2024, 10)

    # 1 month back should be September 2025
    result = calculate_cutoff_date(max_months_lookback=1)
    assert result == (2025, 9)


@patch("f.common_logic.date_utils.datetime")
def test_calculate_cutoff_date_cross_year(mock_datetime):
    """Test calculate_cutoff_date when crossing year boundary"""
    # Mock current date to February 2025
    mock_datetime.now.return_value = datetime(2025, 2, 15)

    # 6 months back from February 2025 should be August 2024
    result = calculate_cutoff_date(max_months_lookback=6)
    assert result == (2024, 8)


def test_calculate_cutoff_date_with_none():
    """Test calculate_cutoff_date with None returns None"""
    result = calculate_cutoff_date(max_months_lookback=None)
    assert result is None


@pytest.mark.parametrize(
    "observation_date, match",
    [
        ("2020-1-21", "YYYY-MM-DD"),
        ("2020/01/21", "YYYY-MM-DD"),
        ("21-01-2020", "YYYY-MM-DD"),
        ("", "YYYY-MM-DD"),
        (None, "YYYY-MM-DD"),
        ("2020-02-31", "real calendar date"),
    ],
)
def test_invalid_observation_date(observation_date, match):
    with pytest.raises(ValueError, match=match):
        parse_observation_date(observation_date)


def test_parse_observation_date_bounds():
    assert parse_observation_date("  2020-01-21  ") == date(2020, 1, 21)
    assert parse_observation_date("1799-12-31") == date(1799, 12, 31)
    today = date.today()
    assert parse_observation_date(today.isoformat()) == today
    with pytest.raises(ValueError, match="future"):
        parse_observation_date((today + timedelta(days=1)).isoformat())


def test_parse_observation_date_earliest():
    earliest = date(1800, 1, 1)
    assert parse_observation_date("1800-01-01", earliest=earliest) == earliest
    with pytest.raises(ValueError, match="on or after 1800-01-01"):
        parse_observation_date("1799-12-31", earliest=earliest)

