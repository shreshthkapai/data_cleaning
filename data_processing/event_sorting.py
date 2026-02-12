"""
Sorting
Sort events using B1's canonical ordering logic
Copied from B1's event sorting in run_b1()
"""

import pandas as pd


def sort_events(df):
    """
    Sort events using B1's canonical ordering.
    
    Sort order (from B1 + light stage priority refinement):
    1. case_id (ascending)
    2. timestamp_utc (ascending)
    3. l1_statuscode_priority (statuscode first for L1 ties)
    4. has_sequence (events with sequence come first: 0 before 1)
    5. sequence value (ascending, if present)
    6. row_index (original order as tiebreaker)
    
    Args:
        df: DataFrame with case_id, timestamp_utc, sequence columns
        
    Returns:
        Sorted DataFrame
    """
    # Optional L1 priority: prefer statuscode when L1 events share a timestamp
    if 'Field' in df.columns and 'bucket' in df.columns:
        df['_l1_statuscode_priority'] = (
            ((df['bucket'] == 'L1_STAGE') & (df['Field'].str.lower() == 'statuscode')).astype(int)
            .rsub(1)
        )
    else:
        df['_l1_statuscode_priority'] = 1

    # Add has_sequence flag (0 if sequence exists, 1 if not)
    df['_has_sequence'] = df['sequence'].isna().astype(int)
    
    # Fill NaN sequences with 0 for sorting
    df['_sequence_value'] = df['sequence'].fillna(0).astype(int)
    
    # Add row_index for stable sort
    df['_row_index'] = range(len(df))
    
    # Sort using B1's exact logic
    df_sorted = df.sort_values(
        by=['case_id', 'timestamp_utc', '_l1_statuscode_priority', '_has_sequence', '_sequence_value', '_row_index'],
        ascending=[True, True, True, True, True, True]
    ).copy()
    
    # Drop temporary columns
    df_sorted = df_sorted.drop(columns=['_l1_statuscode_priority', '_has_sequence', '_sequence_value', '_row_index'])
    
    # Reset index
    df_sorted = df_sorted.reset_index(drop=True)
    
    return df_sorted

