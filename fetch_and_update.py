import os
import requests
import pandas as pd
from datetime import datetime

# === НАСТРОЙКИ ===
TICKERS = ["LQDT", "GOLD", "OBLG", "EQMX"]
DATA_DIR = "data"  # Папка для CSV
os.makedirs(DATA_DIR, exist_ok=True)

def fetch_history(ticker, last_date=None):
    print(f"\n🔹 Обрабатываем {ticker}...")

    url = f"https://iss.moex.com/iss/history/engines/stock/markets/tqtf/securities/{ticker}.json"
    params = {
        "iss.meta": "off",
        "iss.only": "history"
    }
    if last_date:
        params["from"] = last_date  # Можно также добавить "till": last_date

    r = requests.get(url, params=params, headers={"User-Agent": "Mozilla/5.0"})

    print(f"\n🌐 URL запроса: {r.url}")
    print(f"HTTP статус: {r.status_code}")
    print(f"Первые 500 символов ответа MOEX:\n{r.text[:500]}")

    try:
        json_data = r.json()
    except ValueError:
        print(f"❌ Не JSON-ответ от MOEX для {ticker}! Пропускаем.")
        return None

    if "history" not in json_data:
        print(f"⚠ Нет данных в блоке 'history' для {ticker}")
        return None

    columns = json_data["history"]["columns"]
    data = json_data["history"]["data"]
    if not data:
        print(f"⚠ Пустой массив данных для {ticker}")
        return None

    df = pd.DataFrame(data, columns=columns)

    required_cols = ["TRADEDATE", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]
    df = df[[c for c in required_cols if c in df.columns]]

    print(f"✅ Получено строк: {len(df)} для {ticker}")

    return df

# === ГЛАВНАЯ ЛОГИКА ===
for ticker in TICKERS:
    file_path = os.path.join(DATA_DIR, f"{ticker}.csv")

    last_date = None
    if os.path.exists(file_path):
        df_old = pd.read_csv(file_path)
        if "TRADEDATE" in df_old.columns:
            last_date = df_old["TRADEDATE"].max()
            print(f"📅 Последняя дата в {ticker}.csv: {last_date}")

    df_new = fetch_history(ticker, last_date)

    if df_new is None:
        print(f"⚠ Нет новых данных для {ticker}, пропускаем сохранение.")
        continue

    if os.path.exists(file_path):
        df_old = pd.read_csv(file_path)
        df_merged = pd.concat([df_old, df_new]).drop_duplicates(subset=["TRADEDATE"]).sort_values("TRADEDATE")
    else:
        df_merged = df_new

    df_merged.to_csv(file_path, index=False)
    print(f"💾 Сохранено {len(df_merged)} строк → {file_path}")

print("\n✅ Готово!")
