import requests
import pandas as pd
import os
from datetime import datetime

# === Настройки тикеров и начальной даты сбора ===
TICKERS = {
    "LQDT": "2022-01-01",
    "GOLD": "2022-01-01",
    "OBLG": "2022-12-09",
    "EQMX": "2022-01-01"
}

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

def fetch_moex_history(ticker, date_from, date_till):
    url = f"https://iss.moex.com/iss/history/engines/stock/markets/shares/boards/TQTF/securities/{ticker}.xml?from={date_from}&till={date_till}"
    print(f"\n🔹 URL: {url}")

    r = requests.get(url)
    if r.status_code != 200:
        print(f"⚠ Ошибка запроса для {ticker}: {r.status_code}")
        return None

    try:
        df = pd.read_xml(r.text)
        return df
    except Exception as e:
        print(f"⚠ Ошибка парсинга XML: {e}")
        return None

def update_ticker(ticker, start_date):
    file_path = os.path.join(DATA_DIR, f"{ticker}.csv")

    # Определяем начало загрузки
    if os.path.exists(file_path):
        df_old = pd.read_csv(file_path)
        df_old['TRADEDATE'] = pd.to_datetime(df_old['TRADEDATE'])
        last_date = df_old['TRADEDATE'].max().strftime("%Y-%m-%d")
        print(f"📅 Последняя дата в {file_path}: {last_date}")
    else:
        last_date = start_date
        df_old = pd.DataFrame()

    today = datetime.today().strftime("%Y-%m-%d")

    df_new = fetch_moex_history(ticker, last_date, today)

    if df_new is None or df_new.empty:
        print(f"⚠ Нет новых данных для {ticker}")
        return

    # Оставляем только нужные колонки
    cols = ["TRADEDATE", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]
    df_new = df_new[cols]

    # Объединяем с предыдущими данными
    if not df_old.empty:
        df_full = pd.concat([df_old, df_new]).drop_duplicates()
    else:
        df_full = df_new

    df_full.to_csv(file_path, index=False)
    print(f"✅ Обновлено: {file_path} — {len(df_full)} строк")

# === Запуск по всем тикерам ===
if __name__ == "__main__":
    for ticker, start_date in TICKERS.items():
        print(f"\n=== Обрабатываем {ticker} ===")
        update_ticker(ticker, start_date)
