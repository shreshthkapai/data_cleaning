"""
XES Export
Convert cleaned audit log to XES format for process mining tools
Adapted from PRESCIENT b2.py for Dynamics 365 audit log pipeline
"""

import csv
import re
from pathlib import Path
from xml.etree import ElementTree as ET
from datetime import datetime, timedelta


class XESExportError(Exception):
    pass


_TIMESTAMP_UTC_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$")
_XES_NS = "http://www.xes-standard.org/"


def _q(tag: str) -> str:
    """Qualify XML tag with XES namespace"""
    return f"{{{_XES_NS}}}{tag}"


def _append_kv(parent: ET.Element, *, tag: str, key: str, value: str) -> ET.Element:
    """Append key-value element to parent"""
    el = ET.SubElement(parent, _q(tag))
    el.set("key", key)
    el.set("value", value)
    return el


def _validate_timestamp_utc(value: str) -> None:
    """Validate timestamp is strict UTC ISO-8601 ending with Z"""
    if value != value.strip():
        raise XESExportError(f"timestamp_utc must not contain surrounding whitespace (got {value!r})")
    if not value.endswith("Z"):
        raise XESExportError(f"timestamp_utc must be strict UTC ISO-8601 ending with 'Z' (got {value!r})")
    if not _TIMESTAMP_UTC_RE.match(value):
        raise XESExportError(f"timestamp_utc must be strict UTC ISO-8601 ending with 'Z' (got {value!r})")
    
    try:
        dt = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as e:
        raise XESExportError(f"timestamp_utc is not a valid ISO-8601 datetime (got {value!r})") from e
    
    if dt.tzinfo is None or dt.utcoffset() != timedelta(0):
        raise XESExportError(f"timestamp_utc must be UTC (got {value!r})")


def _export_single_xes(df_subset, xes_output_path: Path, bucket_name: str) -> None:
    """Export a single XES file for a bucket subset"""
    # Validate required columns
    required_columns = ['case_id', 'activity_name', 'timestamp_utc']
    missing = [col for col in required_columns if col not in df_subset.columns]
    if missing:
        raise XESExportError(f"DataFrame missing required columns: {missing}")
    
    # Validate timestamps
    for idx, ts in enumerate(df_subset['timestamp_utc']):
        try:
            _validate_timestamp_utc(ts)
        except XESExportError as e:
            raise XESExportError(f"Invalid timestamp at row {idx}: {e}") from e
    
    # Group events by case
    events_by_case = {}
    for _, row in df_subset.iterrows():
        case_id = row['case_id']
        if case_id not in events_by_case:
            events_by_case[case_id] = []
        events_by_case[case_id].append(row)
    
    # Sort case IDs for deterministic output
    case_ids_sorted = sorted(events_by_case.keys())
    
    # Build XES XML
    ET.register_namespace("", _XES_NS)
    root = ET.Element(_q("log"))
    
    for case_id in case_ids_sorted:
        case_events = events_by_case[case_id]
        
        trace_el = ET.SubElement(root, _q("trace"))
        _append_kv(trace_el, tag="string", key="concept:name", value=case_id)
        
        # Events are already sorted by the pipeline (step 10)
        for ev in case_events:
            event_el = ET.SubElement(trace_el, _q("event"))
            _append_kv(event_el, tag="string", key="concept:name", value=ev['activity_name'])
            _append_kv(event_el, tag="date", key="time:timestamp", value=ev['timestamp_utc'])
    
    # Write XES file
    xes_output_path.parent.mkdir(parents=True, exist_ok=True)
    xml_bytes = ET.tostring(root, encoding="utf-8", xml_declaration=True, short_empty_elements=True)
    if not xml_bytes.endswith(b"\n"):
        xml_bytes += b"\n"
    xes_output_path.write_bytes(xml_bytes)
    
    print(f"  {bucket_name}: {len(events_by_case)} cases, {len(df_subset)} events → {xes_output_path.name}")


def export_to_xes(df, output_dir: Path) -> None:
    """
    Export cleaned audit log DataFrame to 3 XES files (one per bucket level).
    
    Args:
        df: DataFrame with case_id, activity_name, timestamp_utc, level columns (already sorted)
        output_dir: Output directory for XES files
        
    Returns:
        None (writes 3 XES files to disk: log_L1.xes, log_L2.xes, log_L3.xes)
    """
    # Validate level column exists
    if 'level' not in df.columns:
        raise XESExportError("DataFrame missing 'level' column - cannot split by bucket")
    
    # Filter by level and export 3 separate XES files
    buckets = {
        'L1': ('L1_STAGE', 'log_L1.xes'),
        'L2': ('L2_MILESTONE', 'log_L2.xes'),
        'L3': ('L3_ADMIN', 'log_L3.xes'),
    }
    
    print("Exporting XES files by bucket level:")
    
    for level_code, (bucket_name, filename) in buckets.items():
        df_level = df[df['level'] == level_code].copy()
        
        if len(df_level) == 0:
            print(f"  {bucket_name}: 0 events, skipping")
            continue
        
        xes_path = output_dir / filename
        _export_single_xes(df_level, xes_path, bucket_name)
