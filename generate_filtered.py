import csv
from datetime import datetime

rows = []
with open('/Users/boghani/Downloads/Line Movement - nba_odds.csv', 'r') as f:
    reader = csv.DictReader(f)
    fieldnames = reader.fieldnames
    for row in reader:
        rows.append(row)

HOURS_BEFORE_FORCE_UPDATE = 4
history = {}
rows_to_write = []

for row in rows:
    key = (row["bookmaker"], row["home_team"], row["away_team"], row["market"], row["team_or_side"])
    new_price = float(row["price"]) if row["price"] else 0
    new_point = float(row["point"]) if row["point"] else None
    current_ts = row["timestamp"]
    
    if key not in history:
        rows_to_write.append(row)
        history[key] = (current_ts, new_price, new_point)
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
    
    if value_changed or time_threshold_met:
        rows_to_write.append(row)
        history[key] = (current_ts, new_price, new_point)

output_path = '/Users/boghani/Downloads/Line Movement - nba_odds_filtered.csv'
with open(output_path, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows_to_write)

print(f"Saved {len(rows_to_write)} rows to: {output_path}")
