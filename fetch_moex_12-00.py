# -*- coding: utf-8 -*-
"""
Скрипт для загрузки часовых свечей MOEX с 01.01.2023
и сохранения только тех, что закрылись в 12:00 MSK.
"""
import requests
import pandas as pd
from datetime import datetime
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
END_DATE = datetime.now().strftime('%Y-%m-%dT23:59:59')
BASE_URL = "https://iss.moex.com/iss/engines/stock/markets/shares/securities"
DATA_DIR = "data"

# --- Функции ---
def fetch_all_candles(secid, interval, from_time, till_time, max_iterations=1000):
    """
    Загружает ВСЕ часовые свечи за период с пагинацией.
    """
    all_candles = []
    start = from_time

    for i in range(max_iterations):
        url = f"{BASE_URL}/{secid.lower()}/candles.json"
        params = {
            'interval': interval,
            'from': start,
            'till': till_time,
            'limit': 1000  # Максимум за один запрос
        }

        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        if 'candles' not in data or 'data' not in data['candles']:
            break

        candles_data = data['candles']['data']
        if not candles_data:
            break

        # Добавляем свечи
        all_candles.extend(candles_data)

        # Последняя свеча в этом ответе — новая точка отсчёта
        last_candle_end = candles_data[-1][7]  # "end" — 8-й элемент
        start = last_candle_end

        # Если достигли конца — выходим
        if last_candle_end >= till_time:
            break

    if not all_candles:
        print(f"⚠️ Нет данных для {secid}")
        return pd.DataFrame()

    # Структура как в ISS MOEX
    columns = ["open", "close", "high", "low", "value", "volume", "begin", "end"]
    df = pd.DataFrame(all_candles, columns=columns)
    df['begin'] = pd.to_datetime(df['begin'])
    df['end'] = pd.to_datetime(df['end'])
    df.sort_values('begin', inplace=True)
    return df

def filter_12h_candles(df):
    """
    Оставляет только свечи, закрывшиеся в 12:00 MSK.
    """
    # Фильтруем по времени окончания свечи
    df_12h = df[df['end'].dt.time == pd.to_datetime('12:00:00').time()].copy()
    print(f"  → Отфильтровано свечей в 12:00: {len(df_12h)}")
    return df_12h

def save_dataframe(df, filename):
    os.makedirs(DATA_DIR, exist_ok=True)
    path = os.path.join(DATA_DIR, filename)
    df.to_csv(path, index=False)
    print(f"  → Сохранено в {path}")

# --- Основной код ---
if __name__ == "__main__":
    print(f"Загрузка данных с {START_DATE} по {END_DATE.split('T')[0]}")
    
    for moex_code, file_prefix in INSTRUMENTS.items():
        print(f"\nОбработка: {moex_code}")
        try:
            # 1. Загрузка всех часовых свечей
            df = fetch_all_candles(moex_code, INTERVAL, START_DATE, END_DATE)

            if df.empty:
                continue

            # 2. Фильтрация по 12:00
            df_12h = filter_12h_candles(df)

            if df_12h.empty:
                print(f"  ⚠️ Нет свечей в 12:00 для {moex_code}")
                continue

            # 3. Сохранение
            filename = f"{file_prefix}_H1_12-00.csv"
            save_dataframe(df_12h, filename)

        except Exception as e:
            print(f"  ❌ Ошибка для {moex_code}: {e}")

    print("\n✅ Загрузка и фильтрация завершены.")
