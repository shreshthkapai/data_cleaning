"""
Sequence extraction
Extract sequence number from stepname prefix (e.g., "14-Qualify" → 14)
"""

import re


def extract_sequence(field_name, new_value):
    """
    Extract sequence number from stepname prefix.
    
    Args:
        field_name: Name of the field
        new_value: New value (e.g., "14-Qualify", "15-Develop")
        
    Returns:
        Sequence number as int, or None if not applicable
    """
    if field_name.lower() != 'stepname':
        return None
    
    # Pattern: number followed by hyphen (e.g., "14-", "15-")
    match = re.match(r'^(\d+)-', str(new_value))
    if match:
        return int(match.group(1))
    
    return None


def add_sequence_numbers(df, field_column='Field', new_value_column='New value'):
    """
    Add sequence column extracted from stepname prefix.
    
    Args:
        df: DataFrame with field and new_value columns
        field_column: Name of field column
        new_value_column: Name of new value column
        
    Returns:
        DataFrame with sequence column added
    """
    df['sequence'] = df.apply(
        lambda row: extract_sequence(row[field_column], row[new_value_column]),
        axis=1
    )
    
    return df
