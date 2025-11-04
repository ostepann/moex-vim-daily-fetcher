import pandas as pd
import requests
import os
from datetime import datetime, timedelta

# Создаём папку data, если её нет
os.makedirs("data", exist_ok=True)

def fetch_candles_for_date_range(ticker, start_date, end_date, interval=1):
    """Fetch minute candles for a given ticker and date range (inclusive)."""
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
    """Оставить только свечи с 09:59 до 10:59 включительно."""
    mask = (
        (df['begin'].dt.time >= pd.Timestamp("09:59").time()) &
        (df['begin'].dt.time <= pd.Timestamp("10:59").time())
    )
    return df[mask].copy()

# === Основная логика ===
TODAY = datetime.now().date()
START_DATE = TODAY - timedelta(days=60)  # включительно
END_DATE = TODAY

print(f"📅 Запрашиваю данные с {START_DATE} по {END_DATE} (последние 60 дней)")

TICKERS = ["GOLD", "EQMX", "OBLG"]

for ticker in TICKERS:
    filename = f"{ticker}_M1_0959_1059.CSV"  # обновлено имя файла
    filepath = os.path.join("data", filename)

    print(f"\n📥 Загружаю {ticker}...")
    df = fetch_candles_for_date_range(ticker, START_DATE, END_DATE, interval=1)

    if df.empty:
        print(f"  → Нет данных для {ticker}")
        continue

    df_filtered = filter_0959_to_1059(df)
    print(f"  → Найдено {len(df_filtered)} свечей с 09:59 до 10:59")

    # Сохраняем (перезаписываем файл)
    df_filtered.to_csv(filepath, index=False, date_format='%Y-%m-%d %H:%M:%S')
    print(f"  → Сохранено: {filepath}")

print("\n✅ Готово!")
