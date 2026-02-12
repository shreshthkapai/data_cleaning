"""
Create row aggregation
Merge all Create operation rows per case into single event
"""

import pandas as pd


def aggregate_create_operations(df, operation_column='Operation'):
    """
    Aggregate all Create operations per case into single event.
    
    Args:
        df: DataFrame with case_id, timestamp_utc, Operation columns
        operation_column: Name of operation column
        
    Returns:
        DataFrame with Create operations aggregated
    """
    # Separate Create operations from other operations
    create_df = df[df[operation_column] == 'Create'].copy()
    other_df = df[df[operation_column] != 'Create'].copy()
    
    if len(create_df) == 0:
        return df
    
    # Group Create operations by case_id so each case contributes at most
    # one canonical "Case Created" event.
    aggregated_creates = []
    
    for case_id, group in create_df.groupby(['case_id']):
        group = group.sort_values(by=['timestamp_utc'], ascending=True)

        # Take first row as template
        agg_row = group.iloc[0].copy()
        
        # Override activity_name to "Case Created"
        agg_row['activity_name'] = 'Case Created'
        
        # Keep earliest sequence if exists
        if 'sequence' in agg_row and pd.notna(agg_row['sequence']):
            agg_row['sequence'] = group['sequence'].min()
        
        aggregated_creates.append(agg_row)
    
    # Combine aggregated Creates with other operations
    if aggregated_creates:
        aggregated_df = pd.DataFrame(aggregated_creates)
        result = pd.concat([aggregated_df, other_df], ignore_index=True)
    else:
        result = other_df
    
    return result

