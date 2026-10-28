import re
from datetime import date, datetime

from dateutil.relativedelta import relativedelta

_DATE_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")


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


def parse_observation_date(
    observation_date: str | None, earliest: date | None = None
) -> date:
    """Parse ``YYYY-MM-DD`` and reject dates that are not usable.

    Parameters
    ----------
    observation_date : str or None
        Calendar date, for example ``"2020-01-21"``.
    earliest : datetime.date, optional
        Inclusive lower bound. When omitted, there is no earliest date.

    Returns
    -------
    datetime.date
        The parsed date.

    Raises
    ------
    ValueError
        If the value is not ``YYYY-MM-DD``, is not a real calendar date,
        is before ``earliest``, or is after today.
    """
    text = observation_date.strip() if isinstance(observation_date, str) else ""
    if not _DATE_RE.fullmatch(text):
        raise ValueError("observation_date must be a date in YYYY-MM-DD format.")
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError("observation_date must be a real calendar date.") from exc
    if earliest is not None and parsed < earliest:
        raise ValueError(
            f"observation_date must be on or after {earliest.isoformat()}."
        )
    if parsed > date.today():
        raise ValueError("observation_date must not be in the future.")
    return parsed

