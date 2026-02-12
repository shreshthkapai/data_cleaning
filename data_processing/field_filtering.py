"""
Field filtering - KILL bucket
Remove rows where field is in KILL_NOISE bucket
"""

import pandas as pd


# KILL_NOISE fields from forensic analysis
KILL_FIELDS = [
    "pricelevelid",
    "exchangerate",
    "pricingerrorcode",
    "skippricecalculation",
    "totaldiscountamount",
    "totaldiscountamount_base",
    "totallineitemdiscountamount",
    "totallineitemdiscountamount_base",
    "transactioncurrencyid",
    "isrevenuesystemcalculated",
]


def filter_kill_fields(df, field_column='Field'):
    """
    Filter out rows where field is in KILL_NOISE bucket.
    
    Args:
        df: DataFrame with field column
        field_column: Name of field column
        
    Returns:
        Filtered DataFrame, count of dropped rows
    """
    before_count = len(df)
    
    # Filter out KILL fields
    df_filtered = df[~df[field_column].isin(KILL_FIELDS)].copy()
    
    dropped_count = before_count - len(df_filtered)
    
    return df_filtered, dropped_count
