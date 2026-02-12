"""
Single-event case removal
Remove cases with fewer than 2 events and export to abandoned_cases report
"""

import pandas as pd
from pathlib import Path


def remove_single_event_cases(df, case_id_column='case_id', output_dir=None):
    """
    Remove cases with fewer than 2 events.
    
    Args:
        df: DataFrame with case_id column
        case_id_column: Name of case ID column
        output_dir: Directory to save abandoned cases report
        
    Returns:
        Filtered DataFrame with only cases having >=2 events
    """
    # Count events per case
    case_event_counts = df[case_id_column].value_counts()
    
    # Identify single-event cases
    single_event_cases = case_event_counts[case_event_counts < 2].index.tolist()
    
    # Filter data
    single_event_df = df[df[case_id_column].isin(single_event_cases)].copy()
    filtered_df = df[~df[case_id_column].isin(single_event_cases)].copy()
    
    # Export abandoned cases if output_dir provided
    if output_dir and len(single_event_df) > 0:
        output_path = Path(output_dir) / "abandoned_cases"
        output_path.mkdir(parents=True, exist_ok=True)
        
        export_path = output_path / "single_event_cases.csv"
        single_event_df.to_csv(export_path, index=False)
    
    return filtered_df, len(single_event_cases), len(single_event_df)
