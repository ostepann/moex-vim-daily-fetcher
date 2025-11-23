# -*- coding: utf-8 -*-
"""
Скрипт для загрузки часовых свечей MOEX с 01.01.2023
и сохранения только тех, что закрылись около 12:00 MSK.
"""
import requests
import pandas as pd
from datetime import datetime, timedelta
import os

# --- Настройки ---
INSTRUMENTS = {
    'OBLG': 'OBLG',
    'EQMX': 'EQMX',
    'GOLD': 'GOLD',
    'LQDT': 'LQDT',
}

INTERVAL = 60  # 1 час
START_DATE = "2023-01-01T00:00:00"
END_DATE = "2023-01-31T23:59:59"  # Только январь 2023 для отладки
BASE_URL = "https://iss.moex.com/iss/engines/stock/markets/shares/securities"
DATA_DIR = "data"

# --- Функции ---
def fetch_all_candles(secid, interval, from_time, till_time, max_iterations=100):
    all_candles = []
    start = from_time

    for i in range(max_iterations):
        url = f"{BASE_URL}/{secid.lower()}/candles.json"
        params = {
            'interval': interval,
            'from': start,
            'till': till_time,
            'limit': 1000
        }

        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        if 'candles' not in data or 'data' not in data['candles']:
            break

        candles_data = data['candles']['data']
        if not candles_data:
            break

        all_candles.extend(candles_data)
        last_candle_end = candles_data[-1][7]
        start = last_candle_end

        if last_candle_end >= till_time:
            break

    if not all_candles:
        return pd.DataFrame()

    columns = ["open", "close", "high", "low", "value", "volume", "begin", "end"]
    df = pd.DataFrame(all_candles, columns=columns)
    df['begin'] = pd.to_datetime(df['begin'])
    df['end'] = pd.to_datetime(df['end'])
    df.sort_values('begin', inplace=True)
    return df

def filter_12h_candles(df):
    """
    Ищем свечи, закрывшиеся ОКОЛО 12:00 MSK (11:59–12:01)
    """
    target_time = pd.to_datetime('12:00:00').time()
    df['end_time'] = df['end'].dt.time
    df['time_diff'] = (pd.to_datetime(df['end_time'].astype(str)) - pd.to_datetime('12:00:00')).abs()
    df_12h = df[df['time_diff'] <= pd.Timedelta(minutes=1)].copy()
    print(f"  → Свечей в 11:59–12:01: {len(df_12h)}")
    
    # Отладка: покажем первые 3 свечи
    if not df.empty:
        print("  → Примеры свечей (первые 3):")
        for _, row in df.head(3).iterrows():
            print(f"      begin={row['begin']}, end={row['end']}")
    return df_12h

def save_dataframe(df, filename):
    os.makedirs(DATA_DIR, exist_ok=True)
    path = os.path.join(DATA_DIR, filename)
    df.to_csv(path, index=False)
    print(f"  → Сохранено в {path} ({len(df)} строк)")

# --- Основной код ---
if __name__ == "__main__":
    print(f"Загрузка данных с {START_DATE} по {END_DATE.split('T')[0]}")
    
    for moex_code, file_prefix in INSTRUMENTS.items():
        print(f"\nОбработка: {moex_code}")
        try:
            df = fetch_all_candles(moex_code, INTERVAL, START_DATE, END_DATE)

            if df.empty:
                print(f"  ⚠️ Нет данных")
                continue

            print(f"  → Всего свечей: {len(df)}")
            df_12h = filter_12h_candles(df)

            if df_12h.empty:
                print(f"  ⚠️ Нет свечей около 12:00")
                continue

            filename = f"{file_prefix}_H1_12-00.csv"
            save_dataframe(df_12h, filename)

        except Exception as e:
            print(f"  ❌ Ошибка для {moex_code}: {e}")

    print("\n✅ Загрузка и фильтрация завершены.")
