from datetime import datetime

from dateutil.relativedelta import relativedelta


def calculate_cutoff_date(max_months_lookback):
    """Calculate the cutoff year/month for filtering data by date.

    Parameters
    ----------
    max_months_lookback : int or None
        Maximum number of months to look back. If None, returns None (no filtering).

    Returns
    -------
    tuple of (int, int) or None
        A tuple of (year, month) representing the cutoff date, or None if no filtering.

    Examples
    --------
    >>> from unittest.mock import patch
    >>> from datetime import datetime
    >>> with patch('f.common_logic.date_utils.datetime') as mock_dt:
    ...     mock_dt.now.return_value = datetime(2025, 10, 15)
    ...     calculate_cutoff_date(6)
    (2025, 4)
    >>> calculate_cutoff_date(None) is None
    True
    """
    if max_months_lookback is None:
        return None

    cutoff_date = datetime.now() - relativedelta(months=max_months_lookback)
    return (cutoff_date.year, cutoff_date.month)

