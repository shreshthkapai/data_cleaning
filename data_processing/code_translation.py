"""
Code translation
Translate Dynamics 365 option set codes to human-readable labels
Microsoft standard option sets only - client will provide custom field translations
"""

# Translation tables for Microsoft standard Dynamics 365 option sets
TRANSLATION_TABLES = {
    "statecode": {
        "0": "Open",
        "1": "Won",
        "2": "Lost",
    },
    "statuscode": {
        "1": "In Progress",
        "2": "On Hold",
        "3": "Won",
        "4": "Canceled",
        "5": "Out-Sold",
    },
}


def translate_value(field_name, value):
    """
    Translate option set code to human-readable label.
    
    Args:
        field_name: Name of the field
        value: Option set code value
        
    Returns:
        Translated label or original value if no translation exists
    """
    field_lower = field_name.lower()
    
    if field_lower in TRANSLATION_TABLES:
        translation_map = TRANSLATION_TABLES[field_lower]
        return translation_map.get(value, value)
    
    return value


def add_translations(df, field_column='Field', new_value_column='New value'):
    """
    Add translated_value column to dataframe.
    
    Args:
        df: DataFrame with field and new_value columns
        field_column: Name of field column
        new_value_column: Name of new value column
        
    Returns:
        DataFrame with translated_value column added
    """
    df['translated_value'] = df.apply(
        lambda row: translate_value(row[field_column], row[new_value_column]),
        axis=1
    )
    
    return df
