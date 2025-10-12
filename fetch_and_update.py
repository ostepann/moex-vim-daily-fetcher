import os
import requests
import pandas as pd
from datetime import datetime

# Список тикеров и названий файлов
tickers = {
    "LQDT": "LQDT.csv",
    "GOLD": "GOLD.csv",
    "OBLG": "OBLG.csv",
    "EQMX": "EQMX.csv"
}

# Папка для хранения CSV
data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

for ticker, filename in tickers.items():
    print(f"🔹 Обрабатываем {ticker}...")

    # Получаем исторические данные с MOEX ISS API
    url = f"https://iss.moex.com/iss/history/engines/stock/markets/tqtf/securities/{ticker}.json"
    params = {
        "from": last_date,
        "till": last_date,
        "iss.meta": "off",
        "iss.only": "history"
    }
    
    r = requests.get(url, params=params, headers={"User-Agent": "Mozilla/5.0"})
    
    try:
        json_data = r.json()
    except ValueError:
        print(f"❌ Не удалось получить JSON по {ticker}. Ответ:\n{r.text[:500]}")
        continue

    json_data = r.json()
    columns = json_data['history']['columns']
    data = json_data['history']['data']

    df_new = pd.DataFrame(data, columns=columns)
    df_new['TRADEDATE'] = pd.to_datetime(df_new['TRADEDATE'])

    file_path = os.path.join(data_dir, filename)

    # Если файл существует, объединяем и оставляем только новые строки
    if os.path.exists(file_path):
        df_existing = pd.read_csv(file_path, parse_dates=['TRADEDATE'])
        df_combined = pd.concat([df_existing, df_new])
        df_combined.drop_duplicates(subset=['TRADEDATE'], inplace=True)
        df_combined.sort_values('TRADEDATE', inplace=True)
        df_combined.to_csv(file_path, index=False)
        print(f"✅ Обновлён файл {filename} ({len(df_combined)} строк)")
    else:
        df_new.sort_values('TRADEDATE', inplace=True)
        df_new.to_csv(file_path, index=False)
        print(f"✅ Создан новый файл {filename} ({len(df_new)} строк)")

print("🎉 Обновление завершено!")
