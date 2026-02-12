"""
Main Pipeline: Dynamics 365 Opportunity Audit Log to XES Conversion
"""

import pandas as pd
from pathlib import Path
import re
from prescient_imports import _normalize_value
from data_processing.timestamp_parsing import parse_timestamp_to_utc_iso_z
from data_processing.remove_main_case_requirement import validate_bundle_without_main_case
from data_processing.single_event_filter import remove_single_event_cases
from data_processing.field_filtering import filter_kill_fields
from data_processing.bucket_classification import add_bucket_classifications
from data_processing.code_translation import add_translations
from data_processing.sequence_extraction import add_sequence_numbers
from data_processing.activity_name_derivation import add_activity_names
from data_processing.create_aggregation import aggregate_create_operations
from data_processing.event_sorting import sort_events
from data_processing.event_deduplication import deduplicate_events
from data_processing.xes_export import export_to_xes

DATASET_PATH = Path(__file__).parent / "dataset.csv"
OUTPUT_DIR = Path(__file__).parent / "outputs"


COLUMN_ALIASES = {
    "timestamp": ["Timestamp", "Date logged", "Date", "Logged At", "Created On"],
    "case_id": ["Anon Item ID", "Item ID", "Case ID", "CaseId"],
    "field": ["Field", "Field Name", "Attribute"],
    "previous_value": ["Previous value", "Old value", "Previous Value"],
    "new_value": ["New value", "New Value"],
    "operation": ["Operation", "Action", "Change Type"],
}


def _normalize_column_name(name):
    """Normalize incoming column names to improve schema matching."""
    return " ".join(str(name).replace("\ufeff", "").split())


def _canonicalize_for_match(name):
    """Build a case-insensitive, punctuation-insensitive key for matching."""
    normalized = _normalize_column_name(name).lower()
    return re.sub(r"[^a-z0-9]+", "", normalized)


def _resolve_columns(df):
    """Resolve canonical pipeline column names to actual dataset column names."""
    normalized_to_actual = {_normalize_column_name(col): col for col in df.columns}
    canonical_to_actual = {_canonicalize_for_match(col): col for col in df.columns}
    resolved = {}

    for canonical_name, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            normalized_alias = _normalize_column_name(alias)
            canonical_alias = _canonicalize_for_match(alias)
            if normalized_alias in normalized_to_actual:
                resolved[canonical_name] = normalized_to_actual[normalized_alias]
                break
            if canonical_alias in canonical_to_actual:
                resolved[canonical_name] = canonical_to_actual[canonical_alias]
                break

    # Heuristic fallback for timestamp column names with extra actor/system prefixes.
    if "timestamp" not in resolved:
        timestamp_candidates = [
            col for col in df.columns
            if "date" in _normalize_column_name(col).lower() and "log" in _normalize_column_name(col).lower()
        ]
        if len(timestamp_candidates) == 1:
            resolved["timestamp"] = timestamp_candidates[0]

    missing = [
        key for key in ("timestamp", "case_id", "field", "previous_value", "new_value", "operation")
        if key not in resolved
    ]
    if missing:
        raise KeyError(
            "Missing required columns for pipeline execution: "
            f"{missing}. Available columns: {list(df.columns)}"
        )

    return resolved


def load_dataset():
    """Load audit log dataset"""
    df = pd.read_csv(DATASET_PATH, dtype=str)
    df = df.fillna("")
    df.columns = [_normalize_column_name(col) for col in df.columns]
    
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].str.strip()
    
    return df


def step0_validate_audit_log_bundle(column_map):
    """Step 0: Validate audit log can work without main_case_table"""
    files_cfg = [{
        "kind": "audit_log",
        "filename": "dataset.csv",
        "case_id_column": column_map["case_id"],
        "timestamp_column": column_map["timestamp"],
        "field_name_column": column_map["field"],
        "old_value_column": column_map["previous_value"],
        "new_value_column": column_map["new_value"],
        "operation_column": column_map["operation"],
    }]
    
    validate_bundle_without_main_case(files_cfg)
    print("Step 0: Validated audit log bundle without main_case_table")


def step1_parse_timestamps(df, column_map):
    """Step 1: Parse timestamps to UTC ISO format"""
    parsed_timestamps = []
    dropped_count = 0
    
    for ts in df[column_map['timestamp']]:
        result, reason = parse_timestamp_to_utc_iso_z(ts, assume_timezone="UTC")
        if result:
            parsed_timestamps.append(result)
        else:
            parsed_timestamps.append(None)
            dropped_count += 1
    
    df['timestamp_utc'] = parsed_timestamps
    print(f"Step 1: Parsed timestamps, dropped {dropped_count} invalid rows")
    
    return df[df['timestamp_utc'].notna()].copy()


def step2_normalize_case_ids(df, column_map):
    """Step 2: Normalize case IDs"""
    df['case_id'] = df[column_map['case_id']].apply(
        lambda x: _normalize_value(str(x), trim_whitespace=True, collapse_whitespace=True)
    )
    print(f"Step 2: Normalized case IDs")
    
    return df


def step3_remove_single_event_cases(df):
    """Step 3: Remove cases with fewer than 2 events"""
    filtered_df, num_cases_removed, num_rows_removed = remove_single_event_cases(
        df, 
        case_id_column='case_id',
        output_dir=OUTPUT_DIR
    )
    
    print(f"Step 3: Removed {num_cases_removed} single-event cases ({num_rows_removed} rows)")
    
    return filtered_df


def step4_filter_kill_fields(df, column_map):
    """Step 4: Filter out KILL_NOISE fields"""
    filtered_df, dropped_count = filter_kill_fields(df, field_column=column_map['field'])
    
    print(f"Step 4: Filtered KILL_NOISE fields, dropped {dropped_count} rows")
    
    return filtered_df


def step5_classify_buckets(df, column_map):
    """Step 5: Classify fields into buckets"""
    df = add_bucket_classifications(df, field_column=column_map['field'])
    
    bucket_counts = df['bucket'].value_counts().to_dict()
    print(f"Step 5: Classified fields into buckets - {bucket_counts}")
    
    return df


def step6_translate_codes(df, column_map):
    """Step 6: Translate Microsoft standard option set codes"""
    df = add_translations(
        df,
        field_column=column_map['field'],
        new_value_column=column_map['new_value']
    )
    
    print(f"Step 6: Translated statecode and statuscode values")
    
    return df


def step7_extract_sequences(df, column_map):
    """Step 7: Extract sequence numbers from stepname prefix"""
    df = add_sequence_numbers(
        df,
        field_column=column_map['field'],
        new_value_column=column_map['new_value']
    )
    
    sequence_count = df['sequence'].notna().sum()
    print(f"Step 7: Extracted {sequence_count} sequence numbers from stepname")
    
    return df


def step8_derive_activity_names(df):
    """Step 8: Derive activity names per bucket"""
    df = add_activity_names(df)
    
    unique_activities = df['activity_name'].nunique()
    print(f"Step 8: Generated {unique_activities} unique activity names")
    
    return df


def step9_aggregate_creates(df, column_map):
    """Step 9: Aggregate Create operations into single event per case"""
    before_count = len(df)
    df = aggregate_create_operations(df, operation_column=column_map['operation'])
    after_count = len(df)
    
    aggregated_count = before_count - after_count
    print(f"Step 9: Aggregated {aggregated_count} Create operation rows")
    
    return df


def step10_sort_events(df):
    """Step 10: Sort events using B1 canonical ordering"""
    df = sort_events(df)
    
    print(f"Step 10: Sorted events by case_id, timestamp, sequence")
    
    return df


def step11_deduplicate_events(df):
    """Step 11: Remove duplicate events"""
    df, duplicates_removed = deduplicate_events(df, key_fields=['case_id', 'activity_name', 'timestamp_utc'])
    
    print(f"Step 11: Removed {duplicates_removed} duplicate events")
    
    return df


def step12_export_to_xes(df):
    """Step 12: Export to XES format"""
    xes_path = OUTPUT_DIR / "log.xes"
    export_to_xes(df, xes_path)
    
    print(f"Step 12: Exported to XES format")
    
    return df


def main():
    """Run pipeline"""
    df = load_dataset()
    column_map = _resolve_columns(df)
    step0_validate_audit_log_bundle(column_map)
    print(f"Loaded {len(df):,} rows")
    
    df = step1_parse_timestamps(df, column_map)
    df = step2_normalize_case_ids(df, column_map)
    df = step3_remove_single_event_cases(df)
    df = step4_filter_kill_fields(df, column_map)
    df = step5_classify_buckets(df, column_map)
    df = step6_translate_codes(df, column_map)
    df = step7_extract_sequences(df, column_map)
    df = step8_derive_activity_names(df)
    df = step9_aggregate_creates(df, column_map)
    df = step10_sort_events(df)
    df = step11_deduplicate_events(df)
    df = step12_export_to_xes(df)
    
    print(f"\nFinal: {len(df):,} rows, exported to XES")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / "cleaned_dataset.csv"
    df.to_csv(output_path, index=False)
    print(f"Saved cleaned dataset to {output_path}")


if __name__ == "__main__":
    main()

