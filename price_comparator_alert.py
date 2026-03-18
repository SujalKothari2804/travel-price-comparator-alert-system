import pandas as pd
import json
import sys
from datetime import datetime

# ── SECTION 1: LOAD DATA ──────────────────────────────────────────────────────

INPUT_FILE  = 'FlixBus_Assignment.xlsx'
OUTPUT_FILE = 'FlixBus_Flagging_Output.csv'
THRESHOLD   = 0.15

df = pd.read_excel(INPUT_FILE)

# ── SECTION 2: NORMALIZE RAW DATA ────────────────────────────────────────────

def normalize_bus_type(bt):
    bt = str(bt).lower()
    if 'sleeper' in bt and 'seater' in bt: return 'seater_sleeper'
    elif 'sleeper' in bt: return 'sleeper'
    elif 'seater' in bt: return 'seater'
    return 'other'

def normalize_ac(row):
    bt = str(row['Bus Type']).lower()
    return 0 if 'non ac' in bt or 'non a/c' in bt else 1

def time_to_minutes(t):
    try:
        parts = str(t).split(':')
        return int(parts[0]) * 60 + int(parts[1])
    except:
        return None

# ── SECTION 3: ADD HELPER COLUMNS ────────────────────────────────────────────

df['bus_type_norm']  = df['Bus Type'].apply(normalize_bus_type)
df['is_ac_norm']     = df.apply(normalize_ac, axis=1)
df['dep_min']        = df['Departure Time'].apply(time_to_minutes)
df['is_night']       = df['dep_min'].apply(lambda d: 1 if d is not None and (d >= 1080 or d <= 360) else 0)
df['Departure Date'] = df['Departure Date'].astype(str).str.strip()

# ── SECTION 4: SPLIT FLIXBUS vs COMPETITORS ──────────────────────────────────

flixbus_df = df[df['Operator'] == 'Flixbus'].copy()
comp_df    = df[df['Operator'] != 'Flixbus'].copy()

# ── SECTION 5: COMPARISON & FLAGGING LOGIC ───────────────────────────────────

results = []

for _, fb_row in flixbus_df.iterrows():

    fb_type   = fb_row['bus_type_norm']
    fb_ac     = fb_row['is_ac_norm']
    fb_dur    = fb_row['Journey Duration (Min)']
    fb_night  = fb_row['is_night']
    fb_price  = fb_row['Weighted Average Price']
    fb_rank   = fb_row['SRP Rank']
    fb_date   = fb_row['Departure Date']
    fb_rating = fb_row['Total Ratings']

    mask = (
        (comp_df['Departure Date']             == fb_date)  &
        (comp_df['bus_type_norm']              == fb_type)  &
        (comp_df['is_ac_norm']                 == fb_ac)    &
        (abs(comp_df['Journey Duration (Min)'] - fb_dur) <= 60) &
        (comp_df['is_night']                   == fb_night)
    )
    comparable = comp_df[mask].copy()

    if len(comparable) < 2:
        mask2 = (
            (comp_df['Departure Date']             == fb_date) &
            (comp_df['bus_type_norm']              == fb_type) &
            (comp_df['is_ac_norm']                 == fb_ac)   &
            (abs(comp_df['Journey Duration (Min)'] - fb_dur) <= 60)
        )
        comparable = comp_df[mask2].copy()

    if len(comparable) == 0:
        results.append({
            'rank'          : fb_rank,
            'date'          : fb_date,
            'bus_type'      : fb_row['Bus Type'],
            'departure_time': str(fb_row['Departure Time']),
            'flixbus_price' : round(fb_price, 2),
            'comp_median'   : None,
            'price_diff'    : None,
            'price_diff_pct': None,
            'flag'          : 'INSUFFICIENT DATA',
            'direction'     : '-',
            'magnitude'     : '-',
            'fb_rating'     : fb_rating,
            'comp_rating'   : None,
            'rating_verdict': '-',
            'run_timestamp' : datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
        continue

    median_price   = comparable['Weighted Average Price'].median()
    price_diff     = fb_price - median_price
    price_diff_pct = price_diff / median_price

    if price_diff_pct > THRESHOLD:
        flag = 'FLAGGED'; direction = 'TOO HIGH'
    elif price_diff_pct < -THRESHOLD:
        flag = 'FLAGGED'; direction = 'TOO LOW'
    else:
        flag = 'OK'; direction = 'Within Range'

    comp_avg_rating = round(comparable['Total Ratings'].mean(), 2)
    rating_diff     = round(fb_rating - comp_avg_rating, 2)

    if abs(rating_diff) < 0.3:
        rating_verdict = 'Similar ratings'
    elif rating_diff > 0 and direction == 'TOO HIGH':
        rating_verdict = 'High price partly justified'
    elif rating_diff > 0 and direction == 'TOO LOW':
        rating_verdict = 'Revenue loss — better rated but cheaper'
    elif rating_diff < 0 and direction == 'TOO HIGH':
        rating_verdict = 'Double red flag — worse rated & pricier'
    elif rating_diff < 0 and direction == 'TOO LOW':
        rating_verdict = 'Low price aligns with lower rating'
    else:
        rating_verdict = 'Similar ratings'

    results.append({
        'rank'          : fb_rank,
        'date'          : fb_date,
        'bus_type'      : fb_row['Bus Type'],
        'departure_time': str(fb_row['Departure Time']),
        'flixbus_price' : round(fb_price, 2),
        'comp_median'   : round(median_price, 2),
        'price_diff'    : round(price_diff, 2),
        'price_diff_pct': round(price_diff_pct * 100, 2),
        'flag'          : flag,
        'direction'     : direction,
        'magnitude'     : f"{abs(price_diff_pct)*100:.1f}% (Rs.{abs(price_diff):.0f})",
        'fb_rating'     : fb_rating,
        'comp_rating'   : comp_avg_rating,
        'rating_verdict': rating_verdict,
        'run_timestamp' : datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    })

# ── SECTION 6: SAVE OUTPUT CSV ───────────────────────────────────────────────

results_df = pd.DataFrame(results)
results_df.to_csv(OUTPUT_FILE, mode='a', header=not pd.io.common.file_exists(OUTPUT_FILE), index=False)

# ── SECTION 7: BUILD ALERT SUMMARY FOR N8N ───────────────────────────────────

flagged = results_df[results_df['flag'] == 'FLAGGED']
ok      = results_df[results_df['flag'] == 'OK']

alert_lines = []
alert_lines.append(f"FlixBus Pricing Alert — {datetime.now().strftime('%d %b %Y %H:%M')}")
alert_lines.append(f"Total listings: {len(results_df)} | Flagged: {len(flagged)} | OK: {len(ok)}")
alert_lines.append("")

if len(flagged) == 0:
    alert_lines.append("All FlixBus prices are within range. No action needed.")
else:
    alert_lines.append(f"--- {len(flagged)} FLAGGED LISTINGS ---")
    alert_lines.append("")
    for _, row in flagged.iterrows():
        alert_lines.append(f"Rank {row['rank']} | Date: {row['date']} | {row['direction']}")
        alert_lines.append(f"  Price: Rs.{row['flixbus_price']} | Market median: Rs.{row['comp_median']} | Diff: {row['magnitude']}")
        alert_lines.append(f"  Rating note: {row['rating_verdict']}")
        alert_lines.append("")

alert_text = "\n".join(alert_lines)

# ── SECTION 8: PRINT OUTPUT FOR N8N TO CAPTURE ───────────────────────────────

output = {
    "has_flags"   : len(flagged) > 0,
    "flag_count"  : len(flagged),
    "ok_count"    : len(ok),
    "alert_text"  : alert_text,
    "output_file" : OUTPUT_FILE,
    "timestamp"   : datetime.now().strftime('%Y-%m-%d %H:%M:%S')
}

print(json.dumps(output))