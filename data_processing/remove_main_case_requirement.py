"""
Remove main_case_table requirement
Modified version of b1's validation to allow audit_log-only bundles
"""

class PipelineB1Error(RuntimeError):
    pass


def validate_bundle_without_main_case(files_cfg):
    """
    Validate bundle structure - allows audit_log without main_case_table.
    Modified from b1.py to remove hard requirement for main_case_table.
    
    Args:
        files_cfg: List of file configurations from mapping_config.json
        
    Returns:
        True if valid
        
    Raises:
        PipelineB1Error if bundle structure is invalid
    """
    if not isinstance(files_cfg, list) or not files_cfg:
        raise PipelineB1Error("mapping_config.files must be a non-empty array")
    
    # Check for main_case_table (optional now)
    main_case = [f for f in files_cfg if isinstance(f, dict) and f.get("kind") == "main_case_table"]
    
    # Check for event sources
    event_sources = [
        f for f in files_cfg
        if isinstance(f, dict) and f.get("kind") in {"activity_table", "audit_log", "transaction_table"}
    ]
    
    # If no main_case_table, we need at least one audit_log with case_id column
    if len(main_case) == 0:
        audit_logs = [f for f in files_cfg if isinstance(f, dict) and f.get("kind") == "audit_log"]
        if not audit_logs:
            raise PipelineB1Error(
                "Bundle must include either main_case_table OR audit_log with case_id column"
            )
        
        # Validate audit logs have case_id and timestamp
        for audit_log in audit_logs:
            if not audit_log.get("case_id_column"):
                raise PipelineB1Error("audit_log must declare case_id_column when main_case_table is absent")
            if not audit_log.get("timestamp_column"):
                raise PipelineB1Error("audit_log must declare timestamp_column when main_case_table is absent")
    
    # Need at least one event source
    if not event_sources:
        raise PipelineB1Error("Bundle must include at least one event source (activity_table/audit_log/transaction_table)")
    
    return True
