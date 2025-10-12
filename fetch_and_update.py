import os
import pandas as pd
import requests
from io import StringIO

# === Настройки ===
TICKERS = {
    "LQDT": "https://www.moex.com/export/derivatives/csv/history.aspx?board=TQTF&code=LQDT",
    "GOLD": "https://www.moex.com/export/derivatives/csv/history.aspx?board=TQTF&code=GOLD",
    "OBLG": "https://www.moex.com/export/derivatives/csv/history.aspx?board=TQTF&code=OBLG",
    "EQMX": "https://www.moex.com/export/derivatives/csv/history.aspx?board=TQTF&code=EQMX"
}

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

def fetch_csv(url):
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        print(f"❌ Ошибка загрузки {url} (HTTP {r.status_code})")
        return None
    # Конвертируем в DataFrame
    df = pd.read_csv(StringIO(r.text), sep=";", decimal=",")
    return df

def process_df(df, ticker):
    # Проверяем нужные колонки
    columns_map = {
        "TRADEDATE": "Date",
        "CLOSE": "Close",
        "OPEN": "Open",
        "HIGH": "High",
        "LOW": "Low",
        "VALUE": "Volume",
        "CHANGE": "PctChange"
    }
    df = df.rename(columns=columns_map)
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], format="%d.%m.%Y")
        df.sort_values("Date", inplace=True)
    return df

# === Основной цикл ===
for ticker, url in TICKERS.items():
    print(f"\n🔹 Обрабатываем {ticker}...")

    df_new = fetch_csv(url)
    if df_new is None:
        print(f"⚠ Не удалось загрузить CSV для {ticker}")
        continue

    df_new = process_df(df_new, ticker)

    file_path = os.path.join(DATA_DIR, f"{ticker}.csv")

    # Если файл существует — объединяем с новыми данными
    if os.path.exists(file_path):
        df_existing = pd.read_csv(file_path, parse_dates=["Date"])
        df_combined = pd.concat([df_existing, df_new])
        df_combined.drop_duplicates(subset=["Date"], inplace=True)
        df_combined.sort_values("Date", inplace=True)
    else:
        df_combined = df_new

    df_combined.to_csv(file_path, index=False)
    print(f"💾 Сохранено {len(df_combined)} строк → {file_path}")

print("\n✅ Все тикеры обработаны!")
