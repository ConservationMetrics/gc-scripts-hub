from datetime import datetime
from unittest.mock import patch

from f.common_logic.date_utils import calculate_cutoff_date


@patch("f.common_logic.date_utils.datetime")
def test_calculate_cutoff_date_with_lookback(mock_datetime):
    """Test calculate_cutoff_date with a lookback period"""
    # Mock current date to October 2025
    mock_datetime.now.return_value = datetime(2025, 10, 15)

    # 6 months back from October 2025 should be April 2025
    result = calculate_cutoff_date(6)
    assert result == (2025, 4)

    # 12 months back should be October 2024
    result = calculate_cutoff_date(12)
    assert result == (2024, 10)

    # 1 month back should be September 2025
    result = calculate_cutoff_date(1)
    assert result == (2025, 9)


@patch("f.common_logic.date_utils.datetime")
def test_calculate_cutoff_date_cross_year(mock_datetime):
    """Test calculate_cutoff_date when crossing year boundary"""
    # Mock current date to February 2025
    mock_datetime.now.return_value = datetime(2025, 2, 15)

    # 6 months back from February 2025 should be August 2024
    result = calculate_cutoff_date(6)
    assert result == (2024, 8)


def test_calculate_cutoff_date_with_none():
    """Test calculate_cutoff_date with None returns None"""
    result = calculate_cutoff_date(None)
    assert result is None

