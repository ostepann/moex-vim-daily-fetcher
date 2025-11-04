import pandas as pd
import requests
import os
from datetime import datetime, timedelta

# Создаём папку data
os.makedirs("data", exist_ok=True)

def fetch_candles(ticker, start_date, end_date, interval=1):
    url = f"https://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker}/candles.json"
    all_rows = []
    current = start_date

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
                print(f"⚠️ Ошибка {ticker} на {day_str}: {e}")
                break

            if len(data) < 2 or not data[1]:
                break

            columns = data[0]['columns']
            rows = data[1]
            all_rows.extend(rows)
            start_offset += len(rows)
            if len(rows) < 500:
                break

        current += timedelta(days=1)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows, columns=columns)
    df['begin'] = pd.to_datetime(df['begin'])
    return df

def filter_0959_to_1059(df):
    return df[
        (df['begin'].dt.time >= pd.Timestamp("09:59").time()) &
        (df['begin'].dt.time <= pd.Timestamp("10:59").time())
    ].copy()

# === Дата: сегодня 4 ноября 2025 ===
TODAY = datetime(2025, 11, 4).date()  # или просто: datetime.now().date()
START_DATE = TODAY - timedelta(days=60)  # 5 сентября 2025
END_DATE = TODAY  # 4 ноября 2025

print(f"📅 Диапазон: {START_DATE} – {END_DATE}")

# 🔑 КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: тикеры в НИЖНЕМ регистре!
tickers_lower = ["gold", "eqmx", "oblg"]

for ticker in tickers_lower:
    output_filename = f"{ticker.upper()}_M1_0959_1059.CSV"
    filepath = os.path.join("data", output_filename)

    print(f"\n📥 Запрашиваю {ticker}...")
    df = fetch_candles(ticker, START_DATE, END_DATE)

    if df.empty:
        print(f"  → Нет данных для {ticker}")
        # Создаём пустой файл с заголовками
        empty = pd.DataFrame(columns=['open', 'close', 'high', 'low', 'value', 'volume', 'begin'])
        empty.to_csv(filepath, index=False)
    else:
        df_filtered = filter_0959_to_1059(df)
        print(f"  → Всего: {len(df)}, после фильтра 09:59–10:59: {len(df_filtered)}")
        df_filtered.to_csv(filepath, index=False, date_format='%Y-%m-%d %H:%M:%S')

    print(f"  → Сохранено: {filepath}")

print("\n✅ Завершено.")
