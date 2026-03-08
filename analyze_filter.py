import csv
from datetime import datetime

rows = []
with open('/Users/boghani/Downloads/Line Movement - nba_odds.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        rows.append(row)

first_ts = rows[0]["timestamp"]
first_batch = sum(1 for r in rows if r["timestamp"] == first_ts)
print(f"First poll ({first_ts}): {first_batch} rows - ALL WRITTEN (first run)")

HOURS_BEFORE_FORCE_UPDATE = 4
history = {}
changes_by_ts = {}

for row in rows:
    key = (row["bookmaker"], row["home_team"], row["away_team"], row["market"], row["team_or_side"])
    new_price = float(row["price"]) if row["price"] else 0
    new_point = float(row["point"]) if row["point"] else None
    current_ts = row["timestamp"]
    
    if key not in history:
        history[key] = (current_ts, new_price, new_point)
        if current_ts not in changes_by_ts:
            changes_by_ts[current_ts] = {"new": 0, "changed": 0, "time": 0, "skipped": 0}
        changes_by_ts[current_ts]["new"] += 1
        continue
    
    last_ts_str, last_price, last_point = history[key]
    value_changed = (new_price != last_price) or (new_point != last_point)
    
    try:
        current_dt = datetime.strptime(current_ts, "%Y-%m-%d %H:%M:%S")
        last_dt = datetime.strptime(last_ts_str, "%Y-%m-%d %H:%M:%S")
        hours_passed = (current_dt - last_dt).total_seconds() / 3600
        time_threshold_met = hours_passed >= HOURS_BEFORE_FORCE_UPDATE
    except:
        time_threshold_met = True
    
    if current_ts not in changes_by_ts:
        changes_by_ts[current_ts] = {"new": 0, "changed": 0, "time": 0, "skipped": 0}
    
    if value_changed:
        changes_by_ts[current_ts]["changed"] += 1
        history[key] = (current_ts, new_price, new_point)
    elif time_threshold_met:
        changes_by_ts[current_ts]["time"] += 1
        history[key] = (current_ts, new_price, new_point)
    else:
        changes_by_ts[current_ts]["skipped"] += 1

print("\nBreakdown by poll:")
print("-" * 85)
total_written = 0
total_skipped = 0
for ts in sorted(changes_by_ts.keys()):
    d = changes_by_ts[ts]
    written = d["new"] + d["changed"] + d["time"]
    total_written += written
    total_skipped += d["skipped"]
    print(f"{ts} | Written: {written:2} | Skipped: {d['skipped']:2} | (new:{d['new']}, changed:{d['changed']}, 4hr:{d['time']})")

print("-" * 85)
print(f"TOTAL: {total_written} written, {total_skipped} skipped ({100*total_skipped/(total_written+total_skipped):.1f}% reduction)")
