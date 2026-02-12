"""
Sorting
Sort events using B1's canonical ordering logic
Copied from B1's event sorting in run_b1()
"""

import pandas as pd


def sort_events(df):
    """
    Sort events using B1's canonical ordering.
    
    Sort order (from B1):
    1. case_id (ascending)
    2. timestamp_utc (ascending)
    3. has_sequence (events with sequence come first: 0 before 1)
    4. sequence value (ascending, if present)
    5. row_index (original order as tiebreaker)
    
    Args:
        df: DataFrame with case_id, timestamp_utc, sequence columns
        
    Returns:
        Sorted DataFrame
    """
    # Add has_sequence flag (0 if sequence exists, 1 if not)
    df['_has_sequence'] = df['sequence'].isna().astype(int)
    
    # Fill NaN sequences with 0 for sorting
    df['_sequence_value'] = df['sequence'].fillna(0).astype(int)
    
    # Add row_index for stable sort
    df['_row_index'] = range(len(df))
    
    # Sort using B1's exact logic
    df_sorted = df.sort_values(
        by=['case_id', 'timestamp_utc', '_has_sequence', '_sequence_value', '_row_index'],
        ascending=[True, True, True, True, True]
    ).copy()
    
    # Drop temporary columns
    df_sorted = df_sorted.drop(columns=['_has_sequence', '_sequence_value', '_row_index'])
    
    # Reset index
    df_sorted = df_sorted.reset_index(drop=True)
    
    return df_sorted
