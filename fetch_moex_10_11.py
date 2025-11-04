import pandas as pd
import requests
import os
from datetime import datetime, timedelta

# Создаём папку data, если её нет
os.makedirs("data", exist_ok=True)

def fetch_candles_for_date_range(ticker, start_date, end_date, interval=1):
    """
    Fetch minute candles from MOEX for a given ticker (lowercase) and date range.
    MOEX returns: {"candles": {"columns": [...], "data": [...]}}
    """
    all_rows = []
    current = start_date

    while current <= end_date:
        day_str = current.strftime("%Y-%m-%d")
        start_offset = 0
        while True:
            url = f"https://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker}/candles.json"
            params = {
                "from": day_str,
                "till": day_str,
                "interval": interval,
                "start": start_offset
            }
            try:
                resp = requests.get(url, params=params, timeout=20)
                resp.raise_for_status()
                raw = resp.json()
            except Exception as e:
                print(f"⚠️ Ошибка при загрузке {ticker} на {day_str}: {e}")
                break

            # Обработка нового формата: {"candles": {"columns": ..., "data": ...}}
            if "candles" not in raw:
                print(f"  → Нет ключа 'candles' в ответе для {ticker} на {day_str}")
                break

            candles = raw["candles"]
            columns = candles.get("columns")
            rows = candles.get("data", [])

            if not columns or not rows:
                break

            all_rows.extend(rows)
            start_offset += len(rows)
            if len(rows) < 500:
                break

        current += timedelta(days=1)

    if not all_rows:
        return pd.DataFrame()

    # Извлекаем колонки и создаём DataFrame
    sample_resp = requests.get(
        f"https://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker}/candles.json",
        params={"from": start_date.strftime("%Y-%m-%d"), "till": start_date.strftime("%Y-%m-%d"), "interval": interval},
        timeout=10
    ).json()
    columns = sample_resp["candles"]["columns"]

    df = pd.DataFrame(all_rows, columns=columns)
    df['begin'] = pd.to_datetime(df['begin'])
    return df

def filter_0959_to_1059(df):
    """Оставить только свечи с 09:59:00 до 10:59:59 включительно."""
    return df[
        (df['begin'].dt.time >= pd.Timestamp("09:59").time()) &
        (df['begin'].dt.time <= pd.Timestamp("10:59").time())
    ].copy()

def ensure_file_exists(filepath, columns):
    """Создать пустой CSV с заголовками, если файл не существует."""
    if not os.path.exists(filepath):
        pd.DataFrame(columns=columns).to_csv(filepath, index=False)

# === Основная логика ===
TODAY = datetime.now().date()
START_DATE = TODAY - timedelta(days=60)
END_DATE = TODAY

print(f"📅 Запрашиваю данные с {START_DATE} по {END_DATE}")

# Тикеры в НИЖНЕМ регистре — как в рабочих URL
RAW_TICKERS = ["gold", "eqmx", "oblg"]

# Получим колонки, сделав один тестовый запрос (для структуры пустого файла)
try:
    test_resp = requests.get(
        "https://iss.moex.com/iss/engines/stock/markets/shares/securities/eqmx/candles.json",
        params={"from": "2025-11-01", "till": "2025-11-01", "interval": 1},
        timeout=10
    ).json()
    COLUMNS = test_resp["candles"]["columns"]
except:
    # fallback
    COLUMNS = ["open", "close", "high", "low", "value", "volume", "begin", "end"]

for ticker in RAW_TICKERS:
    filename = f"{ticker.upper()}_M1_0959_1059.CSV"
    filepath = os.path.join("data", filename)

    print(f"\n📥 Загружаю {ticker}...")
    df = fetch_candles_for_date_range(ticker, START_DATE, END_DATE, interval=1)

    if df.empty:
        print(f"  → Нет данных для {ticker}")
        # Создаём пустой файл с правильными заголовками
        pd.DataFrame(columns=COLUMNS).to_csv(filepath, index=False)
    else:
        df_filtered = filter_0959_to_1059(df)
        print(f"  → Всего: {len(df)}, после фильтра 09:59–10:59: {len(df_filtered)}")
        df_filtered.to_csv(filepath, index=False, date_format='%Y-%m-%d %H:%M:%S')

    print(f"  → Сохранено: {filepath}")

print("\n✅ Готово!")
