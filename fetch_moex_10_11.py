import pandas as pd
import requests
from datetime import datetime, timedelta

# MOEX API does not respect time-of-day in 'from'/'till', so we fetch full days and filter locally
def fetch_full_candles(ticker, start_date, end_date, interval=1):
    """
    Fetch all available minute candles for a ticker between start_date and end_date (inclusive).
    Returns a pandas DataFrame with datetime column 'begin'.
    """
    url = f"https://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker}/candles.json"
    all_rows = []
    current = start_date

    # MOEX has daily limits per request, so we iterate day-by-day to avoid missing data
    while current <= end_date:
        day_str = current.strftime("%Y-%m-%d")
        start_offset = 0
        while True:
            params = {
                "from": day_str,
                "till": day_str,
                "interval": interval,
                "start": start_offset
            }
            try:
                resp = requests.get(url, params=params, timeout=20)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                print(f"⚠️ Error fetching {ticker} on {day_str} (offset {start_offset}): {e}")
                break

            if len(data) < 2 or not data[1]:
                break  # no more data

            columns = data[0]['columns']
            rows = data[1]
            all_rows.extend(rows)
            start_offset += len(rows)

            if len(rows) < 500:  # MOEX returns max ~500 per page
                break

        current += timedelta(days=1)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows, columns=columns)
    df['begin'] = pd.to_datetime(df['begin'])
    return df

def filter_time_range(df, start_time="09:59", end_time="11:00"):
    """Keep rows where 'begin' time is in [start_time, end_time)."""
    start_tm = pd.Timestamp(start_time).time()
    end_tm = pd.Timestamp(end_time).time()
    mask = (df['begin'].dt.time >= start_tm) & (df['begin'].dt.time < end_tm)
    return df[mask].copy()

# Configuration
start = datetime(2025, 9, 1)
end = datetime(2025, 11, 1)  # inclusive

ticker_to_file = {
    "gold": "GOLD_M1_10_11.CSV",
    "eqmx": "EQMX_M1_10_11.CSV",
    "oblg": "OBLG_M1_10_11.CSV"
}

for ticker, filename in ticker_to_file.items():
    print(f"\n📥 Fetching data for '{ticker}' from {start.date()} to {end.date()}")
    df_full = fetch_full_candles(ticker, start, end, interval=1)
    if df_full.empty:
        print(f"  → No data for '{ticker}'. Skipping.")
        continue

    df_filtered = filter_time_range(df_full)
    df_filtered.to_csv(filename, index=False, date_format='%Y-%m-%d %H:%M:%S')
    print(f"  → Saved {len(df_filtered)} rows to {filename}")

print("\n✅ All done!")
