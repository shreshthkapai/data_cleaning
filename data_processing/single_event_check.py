import pandas as pd
from pathlib import Path

df = pd.read_csv("dataset.csv", dtype=str)
df.columns = df.columns.str.strip()

df["Date logged"] = pd.to_datetime(df["Date logged"], format="%d/%m/%Y %H:%M", errors='coerce')
df = df.dropna(subset=["Date logged"])

case_event_counts = df.groupby("Anon Item ID").size()
single_event_cases = case_event_counts[case_event_counts == 1].index.tolist()

single_events_df = df[df["Anon Item ID"].isin(single_event_cases)].copy()

most_recent = single_events_df.sort_values("Date logged", ascending=False).iloc[0]

print("MOST RECENT SINGLE-EVENT CASE")
print("="*50)
print(f"Case ID:        {most_recent['Anon Item ID']}")
print(f"Date:           {most_recent['Date logged']}")
print(f"Operation:      {most_recent['Operation']}")
print(f"Field:          {most_recent['Field']}")
print(f"New value:      {most_recent['New value']}")
print(f"Actor:          {most_recent['Anon Actor']}")

print("\nALL SINGLE-EVENT CASES BY DATE (last 10):")
print("="*50)
recent_10 = single_events_df.nlargest(10, "Date logged")[["Date logged", "Anon Item ID", "Field", "Operation", "Anon Actor"]]
print(recent_10.to_string(index=False))
