"""
Step 1: Extended Timestamp Parsing
Adds %m/%d/%y %H:%M format to handle timestamps like "1/29/26 0:00"
"""

from datetime import datetime, timezone
from zoneinfo import ZoneInfo


# Extended fallback formats - copied from b1.py + new format added
_TIMESTAMP_STRPTIME_FALLBACKS = [
    "%Y/%m/%d %H:%M:%S",
    "%d/%m/%Y %I:%M:%S %p",
    "%d/%m/%Y %I:%M %p",
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%m/%d/%y %H:%M",        # NEW ADDITION FROM B0.py handles "1/29/26 0:00"
]

DROP_TIMESTAMP_MISSING = "DROP_TIMESTAMP_MISSING"
DROP_TIMESTAMP_INVALID = "DROP_TIMESTAMP_INVALID"


def parse_timestamp_to_utc_iso_z(value: str, *, assume_timezone: str) -> tuple[str | None, str | None]:
    """
    Extended timestamp parser from b1.py with additional format support.
    
    Args:
        value: Raw timestamp string
        assume_timezone: Timezone to assume for naive timestamps (e.g., "UTC", "America/New_York")
    
    Returns:
        (timestamp_utc_iso_z, drop_reason)
        - If successful: ("2025-08-15T14:51:00Z", None)
        - If failed: (None, "DROP_TIMESTAMP_MISSING" or "DROP_TIMESTAMP_INVALID")
    """
    raw = value.strip()
    if raw == "" or raw.lower() == "null":
        return None, DROP_TIMESTAMP_MISSING

    s = raw
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    dt: datetime | None = None
    
    # Try ISO format first
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        pass

    # Try fallback formats
    if dt is None:
        for timestamp_format in _TIMESTAMP_STRPTIME_FALLBACKS:
            try:
                dt = datetime.strptime(raw, timestamp_format)
                break
            except ValueError:
                continue

    # If all parsing failed
    if dt is None:
        return None, DROP_TIMESTAMP_INVALID

    # Handle timezone
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo(assume_timezone))

    # Convert to UTC and format
    dt_utc = dt.astimezone(timezone.utc).replace(microsecond=0)
    return dt_utc.isoformat().replace("+00:00", "Z"), None



