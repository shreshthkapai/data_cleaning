"""
Deduplication
Remove duplicate events using B1's dedup logic
Copied from B1's dedupe handling in run_b1()
"""

import pandas as pd


def deduplicate_events(df, key_fields=None):
    """
    Remove duplicate events using B1's dedup logic.
    
    Default key_fields from your plan: [case_id, activity_name, timestamp_utc]
    
    Args:
        df: DataFrame with event data
        key_fields: List of column names to use as dedup key (default: case_id, activity_name, timestamp_utc)
        
    Returns:
        Deduplicated DataFrame, count of duplicates removed
    """
    if key_fields is None:
        key_fields = ['case_id', 'activity_name', 'timestamp_utc']
    
    before_count = len(df)
    
    # Keep first occurrence of each unique combination
    df_deduped = df.drop_duplicates(subset=key_fields, keep='first').copy()
    
    after_count = len(df_deduped)
    duplicates_removed = before_count - after_count
    
    return df_deduped, duplicates_removed
