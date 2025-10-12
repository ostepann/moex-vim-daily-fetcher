import os
import requests
import pandas as pd

# === НАСТРОЙКИ ===
TICKERS = ["LQDT", "GOLD", "OBLG", "EQMX"]
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

def fetch_history(ticker, last_date=None):
    print(f"\n🔹 Обрабатываем {ticker}...")

    url = f"https://iss.moex.com/iss/history/engines/funds/markets/fundsecurities/securities/{ticker}.json"
    params = {
        "iss.meta": "off",
        "iss.only": "history"
    }
    if last_date:
        params["from"] = last_date

    r = requests.get(url, params=params, headers={"User-Agent": "Mozilla/5.0"})
    print(f"🌐 URL запроса: {r.url}")
    print(f"HTTP статус: {r.status_code}")
    print(f"Первые 500 символов ответа:\n{r.text[:500]}")

    try:
        json_data = r.json()
    except ValueError:
        print(f"❌ Не удалось преобразовать ответ в JSON для {ticker}")
        return None

    history = json_data.get("history", {})
    columns = history.get("columns", [])
    data = history.get("data", [])

    if not data:
        print(f"⚠ Пустой массив данных для {ticker}")
        return None

    df = pd.DataFrame(data, columns=columns)
    # Приведём дату к типу datetime
    if "TRADEDATE" in df.columns:
        df["TRADEDATE"] = pd.to_datetime(df["TRADEDATE"])
    return df

# === ГЛАВНАЯ ЛОГИКА ===
for ticker in TICKERS:
    file_path = os.path.join(DATA_DIR, f"{ticker}.csv")
    last_date = None

    if os.path.exists(file_path):
        df_existing = pd.read_csv(file_path)
        if "TRADEDATE" in df_existing.columns:
            last_date = df_existing["TRADEDATE"].max()
            print(f"📅 Последняя дата в {ticker}.csv: {last_date}")

    df_new = fetch_history(ticker, last_date)

    if df_new is None:
        print(f"⚠ Нет новых данных для {ticker}, пропускаем сохранение.")
        continue

    if os.path.exists(file_path):
        df_existing = pd.read_csv(file_path)
        df_merged = pd.concat([df_existing, df_new]).drop_duplicates(subset=["TRADEDATE"]).sort_values("TRADEDATE")
    else:
        df_merged = df_new.sort_values("TRADEDATE")

    df_merged.to_csv(file_path, index=False)
    print(f"💾 Сохранено {len(df_merged)} строк → {file_path}")

print("\n✅ Все тикеры обработаны!")
