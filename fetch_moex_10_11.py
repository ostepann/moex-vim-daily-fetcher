import pandas as pd
import requests
import os
from datetime import datetime, timedelta

# Создаём папку data, если её нет
os.makedirs("data", exist_ok=True)

def fetch_candles_for_date_range(ticker, start_date, end_date, interval=1):
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
                print(f"⚠️ Ошибка загрузки {ticker} на {day_str}: {e}")
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
    mask = (
        (df['begin'].dt.time >= pd.Timestamp("09:59").time()) &
        (df['begin'].dt.time <= pd.Timestamp("10:59").time())
    )
    return df[mask].copy()

# === Основная логика ===
TODAY = datetime.now().date()
START_DATE = TODAY - timedelta(days=60)
END_DATE = TODAY

print(f"📅 Запрашиваю данные с {START_DATE} по {END_DATE}")

TICKERS = ["GOLD", "EQMX", "OBLG"]

for ticker in TICKERS:
    filename = f"{ticker}_M1_0959_1059.CSV"
    filepath = os.path.join("data", filename)

    print(f"\n📥 Обрабатываю {ticker}...")

    # Создаём пустой DataFrame с ожидаемой структурой
    empty_df = pd.DataFrame(columns=[
        'open', 'close', 'high', 'low', 'value', 'volume', 'begin'
    ])

    try:
        df_full = fetch_candles_for_date_range(ticker, START_DATE, END_DATE, interval=1)
        if df_full.empty:
            df_to_save = empty_df
        else:
            df_filtered = filter_0959_to_1059(df_full)
            df_to_save = df_filtered if not df_filtered.empty else empty_df
    except Exception as e:
        print(f"  ❌ Исключение при обработке {ticker}: {e}")
        df_to_save = empty_df

    # ВСЕГДА сохраняем файл (даже пустой)
    df_to_save.to_csv(filepath, index=False, date_format='%Y-%m-%d %H:%M:%S')
    print(f"  → Файл сохранён: {filepath} ({len(df_to_save)} строк)")

print("\n✅ Все файлы созданы.")
