"""
Bucket classification
Classify each field into L1_STAGE, L2_MILESTONE, L3_ADMIN, or KILL_NOISE buckets
"""

# Bucket definitions from forensic analysis
BUCKET_FIELDS = {
    "L1_STAGE": [
        "estimatedclosedate",
        "msdyn_forecastcategory",
        "statuscode",
        "statecode",
        "stepname",
        "processid",
        "salesstagecode",
        "actualclosedate",
        "salesstage",
        "closeprobability",
        "the000g_purchaseorderstatus",
    ],
    "L2_MILESTONE": [
        "the000g_revenuetype",
        "completefinalproposal",
        "developproposal",
        "adx_readyfordistribution",
        "completeinternalreview",
        "identifycustomercontacts",
        "identifypursuitteam",
        "msdyn_ordertype",
        "new_oppotunitytype",
        "presentfinalproposal",
        "presentproposal",
        "adx_feedbackyet",
        "adx_partnercollaboration",
        "adx_partnercreated",
        "captureproposalfeedback",
        "confirminterest",
        "decisionmaker",
        "evaluatefit",
        "filedebrief",
        "identifycompetitors",
        "li_isinfluenced",
        "msdyn_gdproptout",
        "new_evaluatefit2",
        "opportunityratingcode",
        "pursuitdecision",
        "resolvefeedback",
        "the000g__revenuetype",
        "purchasetimeframe",
        "purchaseprocess",
        "new_contracttermmonths",
        "new_contractterm",
    ],
    "L3_ADMIN": [
        "ownerid",
        "owningbusinessunit",
        "the000g_isrenewal",
        "customerid",
        "parentaccountid",
        "participatesinworkflow",
        "prioritycode",
        "sendthankyounote",
        "parentcontactid",
    ],
    "KILL_NOISE": [
        "finaldecisiondate",
        "originatingleadid",
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
    ],
}


def classify_field(field_name):
    """
    Classify field into bucket and level.
    
    Args:
        field_name: Name of the field
        
    Returns:
        (bucket, level) tuple
        - bucket: L1_STAGE, L2_MILESTONE, L3_ADMIN, KILL_NOISE, or UNKNOWN
        - level: L1, L2, L3, KILL, or UNKNOWN
    """
    field_lower = field_name.lower()
    
    for bucket, fields in BUCKET_FIELDS.items():
        if field_lower in [f.lower() for f in fields]:
            if bucket == "L1_STAGE":
                return "L1_STAGE", "L1"
            elif bucket == "L2_MILESTONE":
                return "L2_MILESTONE", "L2"
            elif bucket == "L3_ADMIN":
                return "L3_ADMIN", "L3"
            elif bucket == "KILL_NOISE":
                return "KILL_NOISE", "KILL"
    
    return "UNKNOWN", "UNKNOWN"


def add_bucket_classifications(df, field_column='Field'):
    """
    Add bucket and level columns to dataframe.
    
    Args:
        df: DataFrame with field column
        field_column: Name of field column
        
    Returns:
        DataFrame with bucket and level columns added
    """
    classifications = df[field_column].apply(classify_field)
    df['bucket'] = classifications.apply(lambda x: x[0])
    df['level'] = classifications.apply(lambda x: x[1])
    
    return df

