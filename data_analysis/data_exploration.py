"""
Module: data_audit_step1.py
Purpose: Generate exhaustive frequency distributions and completeness metrics 
         for all columns in Dynamics 365 audit log export.
Outputs: 
  - outputs/field_profiles/column_statistics.json
  - outputs/field_profiles/value_frequencies.csv
Usage: python data_audit_step1.py
"""

import pandas as pd
import json
from pathlib import Path
from collections import Counter
from datetime import datetime

def load_audit_log(filepath: str) -> pd.DataFrame:
    """Load and perform initial column normalization."""
    df = pd.read_csv(filepath, dtype=str)
    df.columns = df.columns.str.strip()
    
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].str.strip()
    
    return df

def compute_column_profile(series: pd.Series, column_name: str) -> dict:
    """Generate statistical profile for a single column."""
    non_null = series.dropna()
    empty_strings = (series == "").sum() if series.dtype == 'object' else 0
    
    profile = {
        "column": column_name,
        "dtype": str(series.dtype),
        "total_rows": len(series),
        "null_count": series.isna().sum(),
        "empty_string_count": int(empty_strings) if empty_strings > 0 else 0,
        "non_null_count": len(non_null),
        "unique_values": series.nunique(),
        "value_frequency": series.value_counts().head(20).to_dict()
    }
    
    if pd.api.types.is_numeric_dtype(series):
        profile.update({
            "min": float(series.min()) if not pd.isna(series.min()) else None,
            "max": float(series.max()) if not pd.isna(series.max()) else None,
            "mean": float(series.mean()) if not pd.isna(series.mean()) else None
        })
    
    if column_name == "Date logged":
        try:
            parsed = pd.to_datetime(non_null, format="%d/%m/%Y %H:%M", errors='coerce')
            profile.update({
                "min_timestamp": parsed.min().isoformat() if not pd.isna(parsed.min()) else None,
                "max_timestamp": parsed.max().isoformat() if not pd.isna(parsed.max()) else None,
                "parse_failure_count": parsed.isna().sum()
            })
        except:
            pass
    
    return profile

def main():
    base_path = Path("outputs/field_profiles")
    base_path.mkdir(parents=True, exist_ok=True)
    
    df = load_audit_log("dataset.csv")
    
    column_profiles = {}
    for col in df.columns:
        column_profiles[col] = compute_column_profile(df[col], col)
    
    with open(base_path / "column_statistics.json", "w") as f:
        json.dump(column_profiles, f, indent=2, default=str)
    
    frequency_rows = []
    for col, profile in column_profiles.items():
        for value, count in profile["value_frequency"].items():
            frequency_rows.append({
                "column": col,
                "value": str(value)[:100],  
                "frequency": count,
                "percentage": round((count / profile["total_rows"]) * 100, 2)
            })
    
    freq_df = pd.DataFrame(frequency_rows)
    freq_df.to_csv(base_path / "value_frequencies.csv", index=False)
    
    print(f"Analysis complete. Profiles written to {base_path.resolve()}")

if __name__ == "__main__":
    main()
