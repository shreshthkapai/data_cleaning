"""
Module: field_bucket_analyzer.py
Purpose: Extract all fields from dataset and suggest bucket classification
         based on data characteristics and heuristics.
Output: Terminal output + outputs/field_bucket_analysis.json
"""

import pandas as pd
import json
from pathlib import Path
from collections import defaultdict

def analyze_field_characteristics(df, field_name):
    """Analyze a field's data characteristics to suggest bucket."""
    field_df = df[df["Field"] == field_name].copy()
    
    total_occurrences = len(field_df)
    cases_affected = field_df["Anon Item ID"].nunique()
    total_cases = df["Anon Item ID"].nunique()
    case_coverage_pct = (cases_affected / total_cases) * 100
    
    unique_new_values = field_df["New value"].nunique()
    unique_old_values = field_df["Previous value"].nunique()
    
    empty_old = (field_df["Previous value"].fillna("").str.strip() == "").sum()
    empty_old_pct = (empty_old / total_occurrences) * 100 if total_occurrences > 0 else 0
    
    operations = field_df["Operation"].value_counts().to_dict()
    
    # Sample values for inspection
    top_new_values = field_df["New value"].value_counts().head(5).index.tolist()
    
    return {
        "field": field_name,
        "total_occurrences": int(total_occurrences),
        "cases_affected": int(cases_affected),
        "case_coverage_pct": round(case_coverage_pct, 2),
        "unique_new_values": int(unique_new_values),
        "unique_old_values": int(unique_old_values),
        "empty_old_pct": round(empty_old_pct, 2),
        "operations": {str(k): int(v) for k, v in operations.items()},
        "top_new_values": [str(v)[:50] for v in top_new_values]
    }

def suggest_bucket(field_name, characteristics):
    """Heuristic-based bucket suggestion."""
    field_lower = field_name.lower()
    coverage = characteristics["case_coverage_pct"]
    occurrences = characteristics["total_occurrences"]
    
    # KILL NOISE - pricing/currency/calculation fields
    kill_keywords = [
        "discount", "base", "currency", "exchange", "price", 
        "calculation", "error", "skip", "total"
    ]
    if any(kw in field_lower for kw in kill_keywords):
        if coverage < 40:  # Low coverage + noise keywords = KILL
            return "KILL_NOISE"
    
    # L1 STAGE - lifecycle, BPF, state management
    l1_keywords = [
        "stepname", "stage", "state", "status", "forecast", 
        "processid", "closedate", "probability"
    ]
    if any(kw in field_lower for kw in l1_keywords):
        return "L1_STAGE"
    
    # L2 MILESTONE - step flags, qualifications, classifications
    l2_keywords = [
        "identify", "confirm", "develop", "present", "complete",
        "capture", "resolve", "file", "pursuit", "decision",
        "evaluate", "proposal", "feedback", "review", "debrief",
        "purchase", "contract", "opportunity", "revenue", "type",
        "influenced", "adx_", "gdpr", "ordertype", "rating"
    ]
    if any(kw in field_lower for kw in l2_keywords):
        # But exclude very low occurrence milestone flags
        if occurrences > 5:  # At least some usage
            return "L2_MILESTONE"
    
    # L3 ADMIN - owner, customer, relationships, low-level admin
    l3_keywords = [
        "owner", "customer", "parent", "account", "contact", 
        "businessunit", "workflow", "participant"
    ]
    if any(kw in field_lower for kw in l3_keywords):
        return "L3_ADMIN"
    
    # DEFAULT: if high coverage but no clear signal -> L3_ADMIN
    # if very low coverage or occurrence -> UNKNOWN (review needed)
    if coverage > 20 or occurrences > 50:
        return "L3_ADMIN"
    else:
        return "UNKNOWN"

def main():
    # Load dataset
    df = pd.read_csv("dataset.csv", dtype=str)
    df.columns = df.columns.str.strip()
    
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].fillna("").str.strip()
    
    df = df.rename(columns={
        "Anon Item ID": "Anon Item ID",
        "Field": "Field",
        "Previous value": "Previous value",
        "New value": "New value",
        "Operation": "Operation"
    })
    
    # Get all unique fields
    all_fields = sorted(df["Field"].unique())
    
    print("=" * 80)
    print("FIELD BUCKET ANALYZER")
    print("=" * 80)
    print(f"\nTotal unique fields in dataset: {len(all_fields)}\n")
    
    # Analyze each field
    field_analysis = []
    for field in all_fields:
        chars = analyze_field_characteristics(df, field)
        suggested_bucket = suggest_bucket(field, chars)
        chars["suggested_bucket"] = suggested_bucket
        field_analysis.append(chars)
    
    # Sort by suggested bucket, then by occurrence
    field_analysis.sort(key=lambda x: (x["suggested_bucket"], -x["total_occurrences"]))
    
    # Group by bucket
    buckets = defaultdict(list)
    for item in field_analysis:
        buckets[item["suggested_bucket"]].append(item)
    
    # Print results
    print("\n" + "=" * 80)
    print("SUGGESTED BUCKET CLASSIFICATION")
    print("=" * 80)
    
    for bucket_name in ["L1_STAGE", "L2_MILESTONE", "L3_ADMIN", "KILL_NOISE", "UNKNOWN"]:
        if bucket_name not in buckets:
            continue
        
        fields_in_bucket = buckets[bucket_name]
        print(f"\n{bucket_name} ({len(fields_in_bucket)} fields):")
        print("-" * 80)
        
        for item in fields_in_bucket:
            print(f"  {item['field']:<40} | Cases: {item['cases_affected']:>4} ({item['case_coverage_pct']:>5.1f}%) | Occurrences: {item['total_occurrences']:>5}")
    
    # Print ALL field names (for easy copy-paste)
    print("\n" + "=" * 80)
    print("ALL FIELD NAMES (alphabetical)")
    print("=" * 80)
    for field in all_fields:
        print(f"  {field}")
    
    # Print Python list format
    print("\n" + "=" * 80)
    print("ALL FIELDS AS PYTHON LIST")
    print("=" * 80)
    print("all_fields = [")
    for field in all_fields:
        print(f'    "{field}",')
    print("]")
    
    # Save detailed analysis
    output_dir = Path("outputs/field_bucket_analysis")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    output = {
        "total_fields": len(all_fields),
        "bucket_counts": {k: len(v) for k, v in buckets.items()},
        "fields_by_bucket": {
            bucket: [
                {
                    "field": item["field"],
                    "occurrences": item["total_occurrences"],
                    "case_coverage_pct": item["case_coverage_pct"],
                    "cases_affected": item["cases_affected"]
                }
                for item in items
            ]
            for bucket, items in buckets.items()
        },
        "detailed_analysis": field_analysis
    }
    
    with open(output_dir / "field_bucket_analysis.json", "w") as f:
        json.dump(output, f, indent=2)
    
    print(f"\n\nDetailed analysis saved to: {output_dir / 'field_bucket_analysis.json'}")
    print("=" * 80)

if __name__ == "__main__":
    main()
