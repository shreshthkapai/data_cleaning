"""
Module: data_audit_comprehensive.py
Purpose: Complete forensic analysis of Dynamics 365 opportunity audit log.
         Case lifecycle, field semantics, temporal patterns, actor behavior,
         sequence integrity, and data quality assessment.
Input: dataset.csv
Outputs: outputs/comprehensive_analysis/
           - case_lifecycle.json
           - field_semantics.json
           - actor_behavior.json
           - temporal_patterns.json
           - sequence_analysis.json
           - single_event_forensics.json
           - creation_patterns.json
           - transition_matrix.json
           - timestamp_analysis.json
           - master_summary.json
"""

import pandas as pd
import numpy as np
import json
from pathlib import Path
from collections import defaultdict
from datetime import datetime

class DynamicsAuditForensics:
    def __init__(self, filepath: str):
        self.df = self._load(filepath)
        self.cases = None
        self.field_index = None
        self.actor_index = None
        self.temporal_index = None
        
    def _load(self, filepath: str) -> pd.DataFrame:
        """Load and normalize audit log."""
        df = pd.read_csv(filepath, dtype=str)
        df.columns = df.columns.str.strip()
        
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].str.strip()
        
        df = df.rename(columns={
            "Anon Item ID": "case_id",
            "Operation": "operation",
            "Field": "field",
            "Previous value": "old_value",
            "New value": "new_value",
            "Anon Actor": "actor",
            "Date logged": "timestamp_raw"
        })
        
        df["old_value"] = df["old_value"].fillna("")
        df["new_value"] = df["new_value"].fillna("")
        
        return df
    
    def _parse_timestamp(self, ts: str):
        """Handle dual format timestamps."""
        if pd.isna(ts) or ts == "":
            return pd.NaT
            
        formats = [
            "%d/%m/%Y %H:%M",
            "%d/%m/%y %H:%M", 
            "%m/%d/%y %H:%M"
        ]
        
        for fmt in formats:
            try:
                return pd.to_datetime(ts, format=fmt)
            except:
                continue
        return pd.NaT
    
    def execute_full_forensic_audit(self):
        """Execute all analysis modules in sequence."""
        print("Starting comprehensive forensic audit...")
        
        self._analyze_timestamps()
        self._analyze_cases()
        self._analyze_fields()
        self._analyze_actors()
        self._analyze_sequences()
        self._analyze_temporal()
        self._analyze_single_event_cases()
        self._analyze_creation_patterns()
        self._analyze_transition_matrix()
        self._generate_master_summary()
        
        self._write_outputs()
        
        return self.master_summary
    
    def _analyze_timestamps(self):
        """Timestamp parsing and format distribution."""
        print("  Analyzing timestamps...")
        
        self.df["timestamp"] = self.df["timestamp_raw"].apply(self._parse_timestamp)
        valid_mask = self.df["timestamp"].notna()
        self.df = self.df[valid_mask].copy()
        
        self.df = self.df.sort_values(["case_id", "timestamp"])
        self.df["timestamp_iso"] = self.df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S")
        
        format_counts = defaultdict(int)
        for ts in self.df["timestamp_raw"]:
            if "/202" in ts:
                format_counts["DD/MM/YYYY HH:MM"] += 1
            elif len(ts.split("/")[0]) == 1 or len(ts.split("/")[1]) == 1:
                format_counts["D/M/YY HH:MM"] += 1
            else:
                format_counts["other"] += 1
        
        self.timestamp_analysis = {
            "rows_after_parse": len(self.df),
            "rows_dropped_invalid": int((~valid_mask).sum()),
            "format_distribution": dict(format_counts),
            "min_timestamp": self.df["timestamp"].min().isoformat() if len(self.df) > 0 else None,
            "max_timestamp": self.df["timestamp"].max().isoformat() if len(self.df) > 0 else None,
            "timespan_days": float((self.df["timestamp"].max() - self.df["timestamp"].min()).days) if len(self.df) > 0 else 0
        }
    
    def _analyze_cases(self):
        """Complete case-level lifecycle analysis."""
        print("  Analyzing cases...")
        
        case_groups = self.df.groupby("case_id")
        
        case_records = []
        for case_id, group in case_groups:
            group = group.sort_values("timestamp")
            
            has_create = any(group["operation"] == "Create")
            create_events = group[group["operation"] == "Create"]
            
            duration = 0
            if len(group) > 1:
                duration = (group["timestamp"].max() - group["timestamp"].min()).total_seconds() / 86400
            
            record = {
                "case_id": case_id,
                "event_count": len(group),
                "unique_fields": group["field"].nunique(),
                "unique_actors": group["actor"].nunique(),
                "first_timestamp": group["timestamp"].min().isoformat(),
                "last_timestamp": group["timestamp"].max().isoformat(),
                "duration_days": round(duration, 2),
                "has_create_event": bool(has_create),
                "create_timestamp": create_events["timestamp"].min().isoformat() if has_create else None,
                "has_stepname": bool("stepname" in group["field"].values),
                "has_statuscode": bool("statuscode" in group["field"].values),
                "has_statecode": bool("statecode" in group["field"].values),
                "has_estimatedclosedate": bool("estimatedclosedate" in group["field"].values),
                "has_actualclosedate": bool("actualclosedate" in group["field"].values),
                "stepname_count": int(group[group["field"] == "stepname"].shape[0]),
                "owner_changes": int(group[group["field"] == "ownerid"].shape[0]),
                "revenue_type_changes": int(group[group["field"] == "the000g_revenuetype"].shape[0]),
                "forecast_changes": int(group[group["field"] == "msdyn_forecastcategory"].shape[0])
            }
            case_records.append(record)
        
        self.cases = pd.DataFrame(case_records)
        self.cases = self.cases.sort_values("event_count", ascending=False)
        
        self.case_analysis = {
            "total_cases": int(len(self.cases)),
            "total_events": int(len(self.df)),
            "cases_by_event_bucket": {
                "1_event": int((self.cases["event_count"] == 1).sum()),
                "2_5_events": int(((self.cases["event_count"] >= 2) & (self.cases["event_count"] <= 5)).sum()),
                "6_10_events": int(((self.cases["event_count"] >= 6) & (self.cases["event_count"] <= 10)).sum()),
                "11_plus_events": int((self.cases["event_count"] >= 11).sum())
            },
            "duration_stats": {
                "min_days": float(self.cases["duration_days"].min()),
                "max_days": float(self.cases["duration_days"].max()),
                "mean_days": float(self.cases["duration_days"].mean()),
                "median_days": float(self.cases["duration_days"].median()),
                "zero_duration_cases": int((self.cases["duration_days"] == 0).sum())
            },
            "field_presence": {
                "has_stepname": int(self.cases["has_stepname"].sum()),
                "has_statuscode": int(self.cases["has_statuscode"].sum()),
                "has_statecode": int(self.cases["has_statecode"].sum()),
                "has_estimatedclosedate": int(self.cases["has_estimatedclosedate"].sum()),
                "has_actualclosedate": int(self.cases["has_actualclosedate"].sum())
            }
        }
    
    def _analyze_fields(self):
        """Complete field semantic analysis with coverage and transition patterns."""
        print("  Analyzing fields...")
        
        field_records = []
        for field in self.df["field"].unique():
            group = self.df[self.df["field"] == field]
            cases_with_field = group["case_id"].nunique()
            
            record = {
                "field": field,
                "occurrence_count": int(len(group)),
                "percentage_of_events": round(len(group) / len(self.df) * 100, 2),
                "cases_affected": int(cases_with_field),
                "percentage_of_cases": round(cases_with_field / len(self.cases) * 100, 2),
                "unique_old_values": int(group["old_value"].nunique()),
                "unique_new_values": int(group["new_value"].nunique()),
                "empty_old_count": int((group["old_value"] == "").sum()),
                "empty_new_count": int((group["new_value"] == "").sum()),
                "operation_breakdown": {k: int(v) for k, v in group["operation"].value_counts().to_dict().items()},
                "top_new_values": {str(k)[:50]: int(v) for k, v in group["new_value"].value_counts().head(5).to_dict().items()},
                "has_prefix": bool(group["new_value"].str.match(r"^\d+-").any()) if field == "stepname" else None
            }
            field_records.append(record)
        
        self.field_analysis = sorted(field_records, key=lambda x: x["occurrence_count"], reverse=True)
        
        field_matrix = self.df.groupby(["case_id", "field"]).size().reset_index(name="count")
        coverage_pivot = pd.crosstab(field_matrix["case_id"], field_matrix["field"])
        coverage_pct = (coverage_pivot > 0).sum() / len(coverage_pivot) * 100
        
        self.field_coverage = {
            "fields_above_50pct": [str(f) for f in coverage_pct[coverage_pct > 50].index.tolist()],
            "fields_25_to_50pct": [str(f) for f in coverage_pct[(coverage_pct > 25) & (coverage_pct <= 50)].index.tolist()],
            "fields_10_to_25pct": [str(f) for f in coverage_pct[(coverage_pct > 10) & (coverage_pct <= 25)].index.tolist()],
            "fields_below_10pct": [str(f) for f in coverage_pct[coverage_pct <= 10].index.tolist()]
        }
    
    def _analyze_actors(self):
        """Actor behavior and workload distribution."""
        print("  Analyzing actors...")
        
        actor_records = []
        for actor in self.df["actor"].unique():
            group = self.df[self.df["actor"] == actor]
            cases_handled = group["case_id"].nunique()
            fields_modified = group["field"].nunique()
            
            record = {
                "actor": actor,
                "event_count": int(len(group)),
                "percentage_of_events": round(len(group) / len(self.df) * 100, 2),
                "cases_handled": int(cases_handled),
                "percentage_of_cases": round(cases_handled / len(self.cases) * 100, 2),
                "unique_fields_modified": int(fields_modified),
                "operation_breakdown": {k: int(v) for k, v in group["operation"].value_counts().to_dict().items()},
                "top_fields": {k: int(v) for k, v in group["field"].value_counts().head(5).to_dict().items()},
                "first_activity": group["timestamp"].min().isoformat(),
                "last_activity": group["timestamp"].max().isoformat(),
                "activity_span_days": int((group["timestamp"].max() - group["timestamp"].min()).days)
            }
            actor_records.append(record)
        
        self.actor_analysis = sorted(actor_records, key=lambda x: x["event_count"], reverse=True)
        
        total_events = len(self.df)
        top_3_events = sum(r["event_count"] for r in self.actor_analysis[:3]) if len(self.actor_analysis) >= 3 else sum(r["event_count"] for r in self.actor_analysis)
        
        self.actor_concentration = {
            "top_1_actor_pct": round(self.actor_analysis[0]["event_count"] / total_events * 100, 2) if self.actor_analysis else 0,
            "top_3_actors_pct": round(top_3_events / total_events * 100, 2),
            "total_actors": len(actor_records)
        }
    
    def _analyze_sequences(self):
        """Stepname sequence integrity and prefix analysis."""
        print("  Analyzing stepname sequences...")
        
        stepname_df = self.df[self.df["field"] == "stepname"].copy()
        
        if len(stepname_df) > 0:
            stepname_df["prefix"] = stepname_df["new_value"].str.extract(r"^(\d+)-")
            stepname_df["stage"] = stepname_df["new_value"].str.replace(r"^\d+-", "", regex=True)
            stepname_df["prefix"] = pd.to_numeric(stepname_df["prefix"], errors="coerce")
            
            prefix_coverage = stepname_df["prefix"].notna().sum()
            
            stage_order = []
            for prefix, group in stepname_df.groupby("prefix"):
                stage = group["stage"].iloc[0] if len(group) > 0 else "unknown"
                stage_order.append({
                    "prefix": int(prefix) if not pd.isna(prefix) else None,
                    "stage": stage,
                    "count": int(len(group))
                })
            stage_order = [s for s in stage_order if s["prefix"] is not None]
            stage_order = sorted(stage_order, key=lambda x: x["prefix"])
            
            violations = []
            for case_id, group in stepname_df.groupby("case_id"):
                if len(group) < 2:
                    continue
                group = group.sort_values("timestamp")
                prefixes = group["prefix"].dropna().tolist()
                if len(prefixes) < 2:
                    continue
                for i in range(1, len(prefixes)):
                    if prefixes[i] < prefixes[i-1]:
                        violations.append({
                            "case_id": case_id,
                            "timestamp": group.iloc[i]["timestamp"].isoformat(),
                            "from_stage": group.iloc[i-1]["new_value"],
                            "to_stage": group.iloc[i]["new_value"]
                        })
            
            self.sequence_analysis = {
                "total_stepname_changes": int(len(stepname_df)),
                "cases_with_stepname": int(stepname_df["case_id"].nunique()),
                "prefix_coverage_count": int(prefix_coverage),
                "prefix_coverage_pct": round(prefix_coverage / len(stepname_df) * 100, 2),
                "stage_order": stage_order,
                "prefix_violations": {
                    "count": len(violations),
                    "cases_affected": len(set(v["case_id"] for v in violations)),
                    "violations": violations[:20]
                }
            }
        else:
            self.sequence_analysis = {
                "total_stepname_changes": 0,
                "note": "No stepname events found"
            }
    
    def _analyze_temporal(self):
        """Temporal patterns: seasonality, day of week, hour of day."""
        print("  Analyzing temporal patterns...")
        
        self.df["hour"] = self.df["timestamp"].dt.hour
        self.df["day_of_week"] = self.df["timestamp"].dt.dayofweek
        self.df["day_name"] = self.df["timestamp"].dt.day_name()
        self.df["month"] = self.df["timestamp"].dt.month
        self.df["date"] = self.df["timestamp"].dt.date
        
        hour_dist = {}
        for hour in range(24):
            count = int((self.df["hour"] == hour).sum())
            if count > 0:
                hour_dist[str(hour)] = count
        
        day_dist = self.df["day_name"].value_counts().to_dict()
        month_dist = self.df["month"].value_counts().sort_index().to_dict()
        
        daily_volumes = self.df.groupby("date").size()
        
        self.temporal_analysis = {
            "hour_distribution": {str(k): int(v) for k, v in hour_dist.items()},
            "day_of_week_distribution": {str(k): int(v) for k, v in day_dist.items()},
            "month_distribution": {str(k): int(v) for k, v in month_dist.items()},
            "daily_volume": {
                "mean_per_day": float(daily_volumes.mean()),
                "max_per_day": int(daily_volumes.max()),
                "min_per_day": int(daily_volumes.min())
            },
            "weekend_activity_pct": round(float((self.df["day_of_week"] >= 5).sum() / len(self.df) * 100), 2),
            "business_hours_pct": round(float(((self.df["hour"] >= 9) & (self.df["hour"] <= 17)).sum() / len(self.df) * 100), 2)
        }
    
    def _analyze_single_event_cases(self):
        """Deep forensic analysis of single-event cases."""
        print("  Analyzing single-event cases...")
        
        if len(self.cases) == 0:
            self.single_event_analysis = {"error": "No cases analyzed"}
            return
            
        single_case_ids = self.cases[self.cases["event_count"] == 1]["case_id"].tolist()
        
        if not single_case_ids:
            self.single_event_analysis = {"total_cases": 0, "percentage_of_cases": 0, "total_events": 0}
            return
            
        single_events = self.df[self.df["case_id"].isin(single_case_ids)]
        
        field_breakdown = single_events["field"].value_counts().to_dict()
        actor_breakdown = single_events["actor"].value_counts().to_dict()
        
        operation_breakdown = single_events["operation"].value_counts().to_dict()
        is_creation = "Create" in operation_breakdown
        creation_count = operation_breakdown.get("Create", 0)
        
        field_analysis = []
        for field, count in field_breakdown.items():
            field_samples = single_events[single_events["field"] == field]
            if len(field_samples) > 0:
                sample_row = field_samples.iloc[0]
                sample_value = sample_row["new_value"]
                if len(sample_value) > 50:
                    sample_value = sample_value[:50] + "..."
                field_analysis.append({
                    "field": field,
                    "count": int(count),
                    "percentage_of_single_events": round(count / len(single_events) * 100, 2),
                    "sample_value": sample_value,
                    "sample_actor": sample_row["actor"]
                })
        
        field_analysis = sorted(field_analysis, key=lambda x: x["count"], reverse=True)
        
        actor_concentration = {}
        if actor_breakdown:
            top_actor = max(actor_breakdown, key=actor_breakdown.get)
            actor_concentration = {
                "top_actor": int(actor_breakdown[top_actor]),
                "top_actor_name": top_actor,
                "unique_actors": len(actor_breakdown)
            }
        
        date_distribution = single_events["timestamp"].dt.date.value_counts().to_dict()
        bulk_dates = {}
        for k, v in date_distribution.items():
            if v > 5:
                bulk_dates[str(k)] = int(v)
        
        sample_cases = []
        for case_id in single_case_ids[:10]:
            case_events = single_events[single_events["case_id"] == case_id]
            if len(case_events) > 0:
                case_row = case_events.iloc[0]
                new_val = case_row["new_value"]
                if len(new_val) > 50:
                    new_val = new_val[:50] + "..."
                sample_cases.append({
                    "case_id": case_id,
                    "operation": case_row["operation"],
                    "field": case_row["field"],
                    "new_value": new_val,
                    "actor": case_row["actor"],
                    "timestamp": case_row["timestamp"].isoformat()
                })
        
        self.single_event_analysis = {
            "total_cases": len(single_case_ids),
            "percentage_of_cases": round(len(single_case_ids) / len(self.cases) * 100, 2),
            "total_events": len(single_events),
            "operation_breakdown": {str(k): int(v) for k, v in operation_breakdown.items()},
            "is_creation_driven": bool(is_creation and creation_count == len(single_events)),
            "creation_count": int(creation_count),
            "non_creation_count": int(len(single_events) - creation_count),
            "field_breakdown_detailed": field_analysis,
            "actor_breakdown": {str(k): int(v) for k, v in actor_breakdown.items()},
            "actor_concentration": actor_concentration,
            "bulk_operation_dates": bulk_dates,
            "sample_cases": sample_cases,
            "hypothesis": {
                "abandoned_opportunities": bool(field_breakdown.get("the000g_revenuetype", 0) > 0 and not is_creation),
                "bulk_assignment": bool(field_breakdown.get("ownerid", 0) > 0 and len(actor_breakdown) <= 2),
                "incomplete_audit_trail": bool(is_creation and creation_count < len(single_case_ids)),
                "creation_only": bool(is_creation and creation_count == len(single_events))
            }
        }
    
    def _analyze_creation_patterns(self):
        """Analysis of Create operation semantics."""
        print("  Analyzing creation patterns...")
        
        create_events = self.df[self.df["operation"] == "Create"]
        
        if len(create_events) == 0:
            self.creation_analysis = {
                "total_create_events": 0,
                "cases_with_create": 0,
                "note": "No creation events found"
            }
            return
        
        creation_fields = create_events["field"].value_counts().to_dict()
        
        self.creation_analysis = {
            "total_create_events": int(len(create_events)),
            "cases_with_create": int(create_events["case_id"].nunique()),
            "create_events_per_case": round(len(create_events) / create_events["case_id"].nunique(), 2) if create_events["case_id"].nunique() > 0 else 0,
            "fields_set_at_creation": {str(k): int(v) for k, v in list(creation_fields.items())[:20]},
            "actors_by_creation": {str(k): int(v) for k, v in create_events["actor"].value_counts().head(10).to_dict().items()}
        }
    
    def _analyze_transition_matrix(self):
        """Field value transition analysis for categorical fields."""
        print("  Analyzing state transitions...")
        
        transition_fields = ["stepname", "statuscode", "statecode", "msdyn_forecastcategory", "the000g_revenuetype"]
        
        transitions = {}
        for field in transition_fields:
            field_df = self.df[self.df["field"] == field].copy()
            if len(field_df) < 2:
                continue
            
            field_df = field_df.sort_values(["case_id", "timestamp"])
            
            from_to = []
            for case_id, group in field_df.groupby("case_id"):
                group = group.sort_values("timestamp")
                values = group["new_value"].tolist()
                for i in range(1, len(values)):
                    from_to.append({
                        "from": values[i-1],
                        "to": values[i]
                    })
            
            if from_to:
                matrix_df = pd.DataFrame(from_to)
                transition_counts = matrix_df.groupby(["from", "to"]).size().reset_index(name="count")
                transition_counts = transition_counts.sort_values("count", ascending=False)
                
                transitions[field] = {
                    "total_transitions": int(len(matrix_df)),
                    "unique_transitions": len(transition_counts),
                    "top_transitions": [
                        {"from": str(r["from"])[:50], "to": str(r["to"])[:50], "count": int(r["count"])}
                        for _, r in transition_counts.head(10).iterrows()
                    ]
                }
        
        self.transition_analysis = transitions
    
    def _generate_master_summary(self):
        """Consolidated findings and recommendations."""
        print("  Generating master summary...")
        
        single_pct = self.case_analysis["cases_by_event_bucket"]["1_event"] / self.case_analysis["total_cases"] * 100
        
        high_signal_fields = [f for f in self.field_coverage["fields_above_50pct"] 
                            if f in ["stepname", "statuscode", "statecode", "msdyn_forecastcategory", 
                                    "ownerid", "estimatedclosedate", "the000g_revenuetype"]]
        
        noise_candidates = []
        for f in self.field_analysis[:50]:
            if any(x in f["field"].lower() for x in ["discount", "base", "currency", "exchange", "price", "total"]):
                if f["percentage_of_cases"] < 30:
                    noise_candidates.append(f["field"])
        
        # Fix: field_breakdown_detailed is a list of dicts, not a dict
        primary_fields = {}
        for item in self.single_event_analysis.get("field_breakdown_detailed", [])[:5]:
            primary_fields[item["field"]] = item["count"]
        
        self.master_summary = {
            "dataset_overview": {
                "total_events": int(len(self.df)),
                "total_cases": int(self.case_analysis["total_cases"]),
                "total_actors": int(len(self.actor_analysis)),
                "total_fields": int(len(self.field_analysis)),
                "timespan_days": int(self.timestamp_analysis["timespan_days"]),
                "date_range": [self.timestamp_analysis["min_timestamp"], self.timestamp_analysis["max_timestamp"]]
            },
            "critical_findings": {
                "single_event_cases": {
                    "count": int(self.case_analysis["cases_by_event_bucket"]["1_event"]),
                    "percentage": round(single_pct, 2),
                    "creation_count": self.single_event_analysis.get("creation_count", 0),
                    "non_creation_count": self.single_event_analysis.get("non_creation_count", 0),
                    "primary_fields": primary_fields,
                    "hypothesis": self.single_event_analysis.get("hypothesis", {})
                },
                "timestamp_quality": {
                    "rows_dropped": int(self.timestamp_analysis["rows_dropped_invalid"]),
                    "parse_success_rate": round((1 - self.timestamp_analysis["rows_dropped_invalid"] / (self.timestamp_analysis["rows_dropped_invalid"] + len(self.df))) * 100, 2),
                    "format_distribution": self.timestamp_analysis["format_distribution"]
                },
                "stepname_integrity": {
                    "prefix_coverage": float(self.sequence_analysis.get("prefix_coverage_pct", 0)),
                    "violation_count": int(self.sequence_analysis.get("prefix_violations", {}).get("count", 0)),
                    "violation_cases": int(self.sequence_analysis.get("prefix_violations", {}).get("cases_affected", 0))
                },
                "actor_concentration": self.actor_concentration,
                "high_signal_fields": high_signal_fields[:10],
                "noise_candidates": noise_candidates[:15]
            },
            "pipeline_implications": {
                "timestamp_handler": "dual_format_required" if self.timestamp_analysis["rows_dropped_invalid"] > 0 else "single_format",
                "case_filtering": "filter_2plus_events_recommended" if single_pct > 20 else "keep_all_cases",
                "sequence_tiebreaker": "prefix_primary_with_timestamp_fallback" if self.sequence_analysis.get("prefix_coverage_pct", 0) > 90 else "timestamp_only",
                "bucket_strategy": {
                    "L1_STAGE": ["stepname", "statecode", "statuscode", "msdyn_forecastcategory"],
                    "L2_MILESTONE": [f for f in self.field_coverage["fields_25_to_50pct"][:15] 
                                    if any(x in f.lower() for x in ["proposal", "develop", "qualify", "identify", "complete", "review", "decision"])],
                    "L3_ADMIN": ["ownerid", "owningbusinessunit", "customerid", "estimatedclosedate", "actualclosedate", 
                                "closeprobability", "new_contractterm", "pricelevelid"],
                    "KILL_NOISE": noise_candidates[:20]
                }
            }
        }    
    def _write_outputs(self):
        """Write all analysis outputs to JSON."""
        output_dir = Path("outputs/comprehensive_analysis")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        outputs = {
            "case_lifecycle.json": self.case_analysis,
            "field_semantics.json": self.field_analysis,
            "actor_behavior.json": self.actor_analysis,
            "temporal_patterns.json": self.temporal_analysis,
            "sequence_analysis.json": self.sequence_analysis,
            "single_event_forensics.json": self.single_event_analysis,
            "creation_patterns.json": self.creation_analysis,
            "transition_matrix.json": self.transition_analysis,
            "timestamp_analysis.json": self.timestamp_analysis,
            "master_summary.json": self.master_summary
        }
        
        for filename, data in outputs.items():
            with open(output_dir / filename, "w") as f:
                json.dump(data, f, indent=2, default=str)
        
        self.cases.to_csv(output_dir / "case_metrics_full.csv", index=False)
        
        print(f"\nComprehensive analysis complete.")
        print(f"Output directory: {output_dir.resolve()}")
        print(f"Files written: {list(outputs.keys())}")

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Dynamics 365 Audit Log Forensic Analysis")
    parser.add_argument("--input", default="dataset.csv", help="Input CSV file path")
    args = parser.parse_args()
    
    forensics = DynamicsAuditForensics(args.input)
    summary = forensics.execute_full_forensic_audit()
    
    print("\nFORENSIC AUDIT MASTER SUMMARY")
    print(f"Dataset: {args.input}")
    print(f"Cases: {summary['dataset_overview']['total_cases']}")
    print(f"Events: {summary['dataset_overview']['total_events']}")
    print(f"Fields: {summary['dataset_overview']['total_fields']}")
    print(f"Actors: {summary['dataset_overview']['total_actors']}")
    print(f"\nCritical Findings:")
    print(f"  Single-event cases: {summary['critical_findings']['single_event_cases']['count']} ({summary['critical_findings']['single_event_cases']['percentage']}%)")
    print(f"    - Creation events: {summary['critical_findings']['single_event_cases'].get('creation_count', 0)}")
    print(f"    - Non-creation events: {summary['critical_findings']['single_event_cases'].get('non_creation_count', 0)}")
    print(f"  Timestamp parse success: {summary['critical_findings']['timestamp_quality']['parse_success_rate']}%")
    print(f"  Stepname prefix coverage: {summary['critical_findings']['stepname_integrity']['prefix_coverage']}%")
    print(f"  Prefix violations: {summary['critical_findings']['stepname_integrity']['violation_count']}")
    print(f"  Actor concentration (top 3): {summary['critical_findings']['actor_concentration']['top_3_actors_pct']}%")

if __name__ == "__main__":
    main()
