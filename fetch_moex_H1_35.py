# -*- coding: utf-8 -*-
"""
Скрипт для загрузки данных с MOEX за последние 7 дней и сохранения
в файлы с ограничением в 35 строк.
"""
import requests
import pandas as pd
from datetime import datetime, timedelta
import os

# --- Настройки ---
INSTRUMENTS = {
    'EQMX': 'EQMX',
    'GOLD': 'GOLG',  # Код для файла (предполагаемый)
    'OBLG': 'OBLG'
}

INTERVAL = 60  # Интервал данных (60 = 1 час)
ROWS_TO_KEEP = 35  # Количество строк для сохранения
BASE_URL = "https://iss.moex.com/iss/engines/stock/markets/shares/securities"
DATA_DIR = "data"

# --- Функции ---
def get_last_calendar_days(n):
    """Возвращает даты начала и конца для последних n календарных дней."""
    today = datetime.now()
    start_date = today - timedelta(days=n-1)
    from_str = start_date.strftime('%Y-%m-%dT00:00:00')
    till_str = today.strftime('%Y-%m-%dT23:59:59')
    return from_str, till_str

def fetch_candles(secid, interval, from_time, till_time):
    """Получает данные по свечам для указанного инструмента."""
    url = f"{BASE_URL}/{secid.lower()}/candles.json"
    params = {'interval': interval, 'from': from_time, 'till': till_time}

    response = requests.get(url, params=params)
    response.raise_for_status()

    data = response.json()
    if 'candles' not in data or 'data' not in data['candles']:
        raise ValueError(f"Неожиданная структура данных для {secid}")

    candles_data = data['candles']['data']
    if not candles_data: # Исправлено: было candles_, должно быть candles_data
        print(f"Предупреждение: Для инструмента {secid} не найдены данные за указанный период.")
        return pd.DataFrame()

    # ИСПОЛЬЗУЕМ ТОЧНУЮ ПОСЛЕДОВАТЕЛЬНОСТЬ ПОЛЕЙ ИЗ ВАШЕГО ВЫВОДА
    columns = ["open", "close", "high", "low", "value", "volume", "begin", "end"]
    
    df = pd.DataFrame(candles_data, columns=columns)
    
    # Преобразуем столбцы begin и end в datetime
    df['begin'] = pd.to_datetime(df['begin'])
    df['end'] = pd.to_datetime(df['end'])
    
    df.sort_values('begin', inplace=True)
    return df

def save_and_truncate(df, filename, rows_to_keep):
    """Сохраняет DataFrame в CSV и оставляет последние N строк."""
    os.makedirs(DATA_DIR, exist_ok=True)
    path = os.path.join(DATA_DIR, filename)

    if os.path.exists(path):
        existing_df = pd.read_csv(path)
        combined_df = pd.concat([existing_df, df], ignore_index=True)
    else:
        combined_df = df

    combined_df.drop_duplicates(subset=['begin'], keep='last', inplace=True)
    combined_df.sort_values('begin', inplace=True)
    final_df = combined_df.tail(rows_to_keep).copy()
    print(f"Данные сохранены в {path}. Оставлено строк: {len(final_df)}")
    final_df.to_csv(path, index=False)

# --- Основной код ---
if __name__ == "__main__":
    from_time, till_time = get_last_calendar_days(7)
    print(f"Загрузка данных с {from_time} до {till_time}")

    for moex_code, file_prefix in INSTRUMENTS.items():
        print(f"\nОбработка инструмента: {moex_code}")
        try:
            df = fetch_candles(moex_code, INTERVAL, from_time, till_time)
            if not df.empty:
                filename = f"{file_prefix}_H1_35.CSV"
                save_and_truncate(df, filename, ROWS_TO_KEEP)
                print(f"Успешно обработан {moex_code}")
            else:
                print(f"Для {moex_code} не было получено новых данных.")
        except requests.exceptions.RequestException as e:
            print(f"Ошибка при запросе к API для {moex_code}: {e}")
        except ValueError as e:
            print(f"Ошибка при обработке данных для {moex_code}: {e}")
        except Exception as e:
            print(f"Неизвестная ошибка при обработке {moex_code}: {e}")

    print("\nЗагрузка завершена.")
