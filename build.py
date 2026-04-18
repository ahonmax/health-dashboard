#!/usr/bin/env python3
"""Rebuild health_dashboard.html DATA from both App Data folders."""
import csv, json, re, os, glob
from datetime import datetime, timedelta
from collections import defaultdict

FOLDERS = [
    "/Users/maxahonen/Desktop/data/App Data",
    "/Users/maxahonen/Desktop/data 2/App Data",  # newer, takes precedence
]
GLUCOSE_CSVS = glob.glob("/Users/maxahonen/Desktop/data*/MaxAhonen_glucose_*.csv") + \
               glob.glob("/Users/maxahonen/Desktop/data*/*.csv")
GLUCOSE_CSVS = [f for f in set(GLUCOSE_CSVS) if "glucose" in f.lower() or "Glucose" in f]

HTML_PATH = "/Users/maxahonen/Desktop/data/health_dashboard.html"

def read_csv(path):
    rows = []
    try:
        with open(path, newline='', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f, delimiter=';')
            for row in reader:
                rows.append(row)
    except Exception as e:
        print(f"  WARN: {path}: {e}")
    return rows

def parse_json_field(s):
    if not s:
        return {}
    s = s.strip().strip('"')
    s = s.replace('""', '"')
    try:
        return json.loads(s)
    except:
        return {}

def parse_list_field(s):
    if not s:
        return []
    s = s.strip().strip('"').replace('""', '"')
    try:
        d = json.loads(s)
        if isinstance(d, dict) and 'items' in d:
            return d['items']
        return d
    except:
        return []

def merge_csv_by_day(filename, day_col='day'):
    merged = {}
    for folder in FOLDERS:
        path = os.path.join(folder, filename)
        if not os.path.exists(path):
            continue
        for row in read_csv(path):
            day = row.get(day_col, '').strip()[:10]
            if day:
                merged[day] = row  # later folder wins
    return merged

def merge_sleepmodel():
    """Merge sleepmodel, keeping long_sleep preferred per day."""
    by_day = defaultdict(list)
    for folder in FOLDERS:
        path = os.path.join(folder, 'sleepmodel.csv')
        if not os.path.exists(path):
            continue
        for row in read_csv(path):
            day = row.get('day', '').strip()[:10]
            if day:
                by_day[day].append(row)
    result = {}
    for day, rows in by_day.items():
        long = [r for r in rows if r.get('type') == 'long_sleep']
        chosen = long[0] if long else sorted(rows, key=lambda r: int(r.get('total_sleep_duration') or 0), reverse=True)[0]
        result[day] = chosen
    return result

def load_glucose():
    """Returns dict: day -> list of [minute_of_day, mmol_value]"""
    by_day = defaultdict(list)
    for path in GLUCOSE_CSVS:
        try:
            with open(path, newline='', encoding='utf-8-sig') as f:
                lines = f.readlines()
            # Find header row
            header_idx = None
            for i, line in enumerate(lines):
                if 'Device' in line and 'Record Type' in line:
                    header_idx = i
                    break
            if header_idx is None:
                continue
            reader = csv.DictReader(lines[header_idx:], delimiter=',')
            for row in reader:
                ts = row.get('Device Timestamp', '').strip()
                val = row.get('Historic Glucose mmol/L', '').strip()
                rtype = row.get('Record Type', '').strip()
                if rtype != '0' or not val or not ts:
                    continue
                try:
                    # Parse "08-12-2022 10:13" format
                    dt = datetime.strptime(ts, '%d-%m-%Y %H:%M')
                    day = dt.strftime('%Y-%m-%d')
                    minute = dt.hour * 60 + dt.minute
                    by_day[day].append([minute, round(float(val), 1)])
                except:
                    pass
        except Exception as e:
            print(f"  WARN glucose {path}: {e}")
    # Sort each day's readings by minute
    for day in by_day:
        by_day[day].sort(key=lambda x: x[0])
    return by_day

def rolling_14d_avg(days_sorted, day_vals, current_day):
    """14-day rolling average ending at current_day (exclusive)."""
    idx = days_sorted.index(current_day) if current_day in days_sorted else -1
    if idx < 1:
        return None
    window = [day_vals[d] for d in days_sorted[max(0, idx-14):idx] if d in day_vals and day_vals[d]]
    if not window:
        return None
    return round(sum(window) / len(window), 1)

def build_data():
    print("Loading CSVs...")
    readiness   = merge_csv_by_day('dailyreadiness.csv')
    sleep_score = merge_csv_by_day('dailysleep.csv')
    activity    = merge_csv_by_day('dailyactivity.csv')
    stress      = merge_csv_by_day('dailystress.csv')
    resilience  = merge_csv_by_day('dailyresilience.csv')
    spo2        = merge_csv_by_day('dailyspo2.csv')
    workouts_raw = defaultdict(list)
    for folder in FOLDERS:
        path = os.path.join(folder, 'workout.csv')
        if not os.path.exists(path):
            continue
        for row in read_csv(path):
            day = row.get('day', '').strip()[:10]
            if day:
                workouts_raw[day].append(row)
    sleepmodel  = merge_sleepmodel()
    glucose     = load_glucose()

    all_days = sorted(set(
        list(readiness.keys()) + list(sleep_score.keys()) +
        list(activity.keys()) + list(sleepmodel.keys())
    ))

    # Precompute 14d avg series
    hr_by_day  = {d: float(sleepmodel[d]['average_heart_rate']) for d in sleepmodel if sleepmodel[d].get('average_heart_rate') and float(sleepmodel[d]['average_heart_rate'] or 0) > 0}
    hrv_by_day = {d: float(sleepmodel[d]['average_hrv'])        for d in sleepmodel if sleepmodel[d].get('average_hrv')        and float(sleepmodel[d]['average_hrv'] or 0)        > 0}

    data = {}
    for day in all_days:
        entry = {'day': day}

        # Readiness
        r = readiness.get(day)
        if r:
            contrib = parse_json_field(r.get('contributors', ''))
            entry['readiness'] = {
                'score': int(r['score']) if r.get('score') else None,
                'temp_deviation': float(r['temperature_deviation']) if r.get('temperature_deviation') else None,
                'contributors': {k: int(v) if v else None for k, v in contrib.items()},
            }

        # Sleep score + contributors
        ss = sleep_score.get(day)
        if ss:
            entry['sleep_score'] = int(ss['score']) if ss.get('score') else None
            contrib = parse_json_field(ss.get('contributors', ''))
            entry['sleep_contributors'] = {k: int(v) if v else None for k, v in contrib.items()}

        # Sleep detail from sleepmodel
        sm = sleepmodel.get(day)
        if sm:
            hr_items  = parse_list_field(sm.get('heart_rate', ''))
            hrv_items = parse_list_field(sm.get('hrv', ''))
            entry['sleep'] = {
                'bedtime_start':  sm.get('bedtime_start', ''),
                'bedtime_end':    sm.get('bedtime_end', ''),
                'total_sleep':    int(sm['total_sleep_duration']) if sm.get('total_sleep_duration') else 0,
                'deep':           int(sm['deep_sleep_duration'])  if sm.get('deep_sleep_duration')  else 0,
                'light':          int(sm['light_sleep_duration']) if sm.get('light_sleep_duration') else 0,
                'rem':            int(sm['rem_sleep_duration'])   if sm.get('rem_sleep_duration')   else 0,
                'awake':          int(sm['awake_time'])           if sm.get('awake_time')           else 0,
                'efficiency':     int(sm['efficiency'])           if sm.get('efficiency')           else None,
                'latency':        int(sm['latency'])              if sm.get('latency')              else 0,
                'avg_hr':         float(sm['average_heart_rate']) if sm.get('average_heart_rate') and float(sm['average_heart_rate'] or 0) > 0 else None,
                'low_hr':         int(sm['lowest_heart_rate'])    if sm.get('lowest_heart_rate')   and int(sm['lowest_heart_rate'] or 0) > 0 else None,
                'avg_hrv':        float(sm['average_hrv'])        if sm.get('average_hrv')         and float(sm['average_hrv'] or 0) > 0 else None,
                'avg_breath':     float(sm['average_breath'])     if sm.get('average_breath')      and float(sm['average_breath'] or 0) > 0 else None,
                'restless':       int(sm['restless_periods'])     if sm.get('restless_periods')    else 0,
                'phases':         sm.get('sleep_phase_5_min', ''),
                'hr_items':       hr_items,
                'hrv_items':      hrv_items,
                'hr_14d_avg':     rolling_14d_avg(all_days, hr_by_day, day),
                'hrv_14d_avg':    rolling_14d_avg(all_days, hrv_by_day, day),
            }

        # Activity
        a = activity.get(day)
        if a:
            contrib = parse_json_field(a.get('contributors', ''))
            entry['activity'] = {
                'score':           int(a['score'])           if a.get('score')           else None,
                'steps':           int(a['steps'])           if a.get('steps')           else 0,
                'active_calories': int(a['active_calories']) if a.get('active_calories') else 0,
                'total_calories':  int(a['total_calories'])  if a.get('total_calories')  else 0,
                'contributors':    {k: int(v) if v else None for k, v in contrib.items()},
                'class_5min':      a.get('class_5_min', ''),
                'class_start':     a.get('timestamp', ''),
            }

        # Workouts
        wlist = []
        for w in workouts_raw.get(day, []):
            wlist.append({
                'activity': w.get('activity', ''),
                'start':    w.get('start_datetime', ''),
                'end':      w.get('end_datetime', ''),
                'calories': float(w['calories']) if w.get('calories') else None,
                'distance': float(w['distance']) if w.get('distance') else None,
            })
        if wlist:
            entry['workouts'] = wlist

        # Stress
        st = stress.get(day)
        if st:
            entry['stress'] = {
                'stress_high':   int(st['stress_high'])   if st.get('stress_high')   else 0,
                'recovery_high': int(st['recovery_high']) if st.get('recovery_high') else 0,
            }

        # Resilience
        res = resilience.get(day)
        if res:
            entry['resilience'] = res.get('level', '')

        # SpO2
        sp = spo2.get(day)
        if sp:
            pct = sp.get('spo2_percentage', '')
            if pct:
                try:
                    pct_val = json.loads(pct.replace('""','"'))
                    avg = pct_val.get('average') if isinstance(pct_val, dict) else None
                    if avg:
                        entry['spo2'] = round(float(avg), 1)
                except:
                    pass

        # Glucose
        gluc = glucose.get(day, [])
        if gluc:
            entry['gluc'] = gluc
            vals = [v for _, v in gluc if v > 0]
            if vals:
                entry['gluc_avg'] = round(sum(vals) / len(vals), 1)
                entry['gluc_min'] = round(min(vals), 1)
                entry['gluc_max'] = round(max(vals), 1)

        data[day] = entry

    return data

def main():
    data = build_data()
    print(f"Built {len(data)} days, {min(data.keys())} to {max(data.keys())}")

    json_str = json.dumps(data, separators=(',', ':'), ensure_ascii=False)

    with open(HTML_PATH) as f:
        html = f.read()

    new_html = re.sub(r'const DATA=\{.*?\};', f'const DATA={json_str};', html, flags=re.DOTALL)

    if new_html == html:
        print("ERROR: pattern not found in HTML")
        return

    with open(HTML_PATH, 'w') as f:
        f.write(new_html)

    print(f"Updated {HTML_PATH}")

if __name__ == '__main__':
    main()
