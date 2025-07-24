import dateparser
from datetime import datetime

def normalize_date(date_str: str) -> str | None:
    if not isinstance(date_str, str) or not date_str.strip():
        return None

    parsed = dateparser.parse(
        date_str,
        languages=['ru'],
        settings={
            'DATE_ORDER': 'DMY',
            'PREFER_DAY_OF_MONTH': 'first',
            'RELATIVE_BASE': datetime(1900, 1, 1),
        }
    )
    if parsed is None:
        return None

    return parsed.strftime('%d-%m-%Y')

def test_valid_dates():
    assert normalize_date("20.9.1901") == "20-09-1901"
    assert normalize_date("6 мая 1999") == "06-05-1999"
    assert normalize_date("31.07.1988") == "31-07-1988"
    assert normalize_date("5 августа 1982") == "05-08-1982"

def test_empty_and_none():
    assert normalize_date("") is None
    assert normalize_date("   ") is None
    assert normalize_date(None) is None
    assert normalize_date(123) is None

def test_invalid_dates():
    assert normalize_date("31.2.1930") is None
    assert normalize_date("666.5") is None
    assert normalize_date("29.2.1983") is None
