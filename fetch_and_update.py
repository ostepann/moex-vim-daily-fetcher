import requests
import pandas as pd
import xml.etree.ElementTree as ET
import os
from datetime import datetime
import time

TICKERS = {
    "LQDT": ("2022-01-01", "fund"),
    "GOLD": ("2022-07-01", "fund"),  # ✅ Корректный тикер золота
    "OBLG": ("2022-12-09", "fund"),
    "EQMX": ("2022-01-01", "fund"),
    "RVI":  ("2022-01-01", "index")
}

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

MAX_RETRIES = 5
RETRY_DELAY = 5

def fetch_moex_history_paginated(ticker, date_from, date_till, asset_type="fund"):
    all_rows = []
    start = 0

    if asset_type == "index":
        base_url = f"https://iss.moex.com/iss/history/engines/stock/markets/index/boards/RTSI/securities/{ticker}.xml"
    else:
        base_url = f"https://iss.moex.com/iss/history/engines/stock/markets/shares/boards/TQTF/securities/{ticker}.xml"

    while True:
        url = f"{base_url}?from={date_from}&till={date_till}&start={start}"
        print(f"🔹 Запрос: {url}")

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = requests.get(url, timeout=10)
                if r.status_code != 200:
                    raise Exception(f"HTTP {r.status_code}")
                root = ET.fromstring(r.text)
                break
            except Exception as e:
                print(f"⚠ Ошибка (попытка {attempt}/{MAX_RETRIES}): {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
                else:
                    print(f"❌ Пропускаем {ticker} (start={start})")
                    return pd.DataFrame()

        rows = root.findall(".//row")
        row_count = len(rows)

        if row_count == 0:
            break

        for row in rows:
            all_rows.append(row.attrib)

        print(f"  Получено {row_count} строк (start={start})")

        if row_count < 100:
            break

        start += 100

    if not all_rows:
        return pd.DataFrame()
    
    df = pd.DataFrame(all_rows)
    required_cols = ["TRADEDATE", "OPEN", "HIGH", "LOW", "CLOSE"]
    if "VOLUME" in df.columns:
        cols = required_cols + ["VOLUME"]
    else:
        cols = required_cols
        df["VOLUME"] = 0

    df = df[cols]
    df['TRADEDATE'] = pd.to_datetime(df['TRADEDATE'])
    return df


def update_ticker(ticker, start_date, asset_type):
    file_path = os.path.join(DATA_DIR, f"{ticker}.csv")

    if os.path.exists(file_path):
        df_old = pd.read_csv(file_path)
        df_old['TRADEDATE'] = pd.to_datetime(df_old['TRADEDATE'])
        last_date = df_old['TRADEDATE'].max().strftime("%Y-%m-%d")
        print(f"📅 Последняя дата в {file_path}: {last_date}")
    else:
        df_old = pd.DataFrame()
        last_date = start_date

    today = datetime.today().strftime("%Y-%m-%d")
    df_new = fetch_moex_history_paginated(ticker, last_date, today, asset_type)

    if df_new.empty:
        print(f"⚠ Нет новых данных для {ticker}")
        return

    if not df_old.empty:
        df_full = pd.concat([df_old, df_new]).drop_duplicates(subset="TRADEDATE").sort_values("TRADEDATE")
    else:
        df_full = df_new.sort_values("TRADEDATE")

    df_full.to_csv(file_path, index=False)
    print(f"✅ Обновлено: {file_path} — {len(df_new)} новых строк")


if __name__ == "__main__":
    for ticker, (start_date, asset_type) in TICKERS.items():
        print(f"\n=== Обрабатываем {ticker} ({asset_type}) ===")
        update_ticker(ticker, start_date, asset_type)
