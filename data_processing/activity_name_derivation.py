"""
Activity name derivation
Create activity names based on bucket classification
Based on B1's _derive_audit_activity_name with bucket-specific logic
"""


def derive_activity_name(row):
    """
    Derive activity name based on bucket classification.
    Modified from B1's _derive_audit_activity_name to support bucket levels.
    
    Args:
        row: DataFrame row with Field, translated_value, bucket columns
        
    Returns:
        Activity name string
    """
    field = row['Field']
    bucket = row['bucket']
    
    # Use translated_value if available, otherwise use New value
    value = row.get('translated_value', row.get('New value', ''))
    
    # L1_STAGE: "Field → Value" format
    if bucket == 'L1_STAGE':
        return f"{field} → {value}"
    
    # L2_MILESTONE: "Field changed" or "Field → Value" for important fields
    elif bucket == 'L2_MILESTONE':
        # Boolean milestone flags (developproposal, completefinalproposal, etc.)
        if field.lower() in ['developproposal', 'completefinalproposal', 'completeinternalreview',
                             'identifycustomercontacts', 'identifypursuitteam', 'presentfinalproposal',
                             'presentproposal', 'confirminterest', 'decisionmaker', 'evaluatefit',
                             'filedebrief', 'pursuitdecision', 'captureproposalfeedback', 'resolvefeedback',
                             'identifycompetitors']:
            return f"{field} completed"
        # Other milestones show value
        else:
            return f"{field} → {value}"
    
    # L3_ADMIN: "Field changed" format
    elif bucket == 'L3_ADMIN':
        if field.lower() == 'ownerid':
            return "Owner changed"
        else:
            return f"{field} changed"
    
    # Fallback
    return f"{field} → {value}"


def add_activity_names(df):
    """
    Add activity_name column based on bucket classification.
    
    Args:
        df: DataFrame with Field, New value, translated_value, bucket columns
        
    Returns:
        DataFrame with activity_name column added
    """
    df['activity_name'] = df.apply(derive_activity_name, axis=1)
    
    return df
