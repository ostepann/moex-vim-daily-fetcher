import requests
import pandas as pd
import xml.etree.ElementTree as ET
import os
from datetime import datetime, timedelta
import time
import random

# Словарь тикеров: тикер -> (дата_начала, тип_актива, борд)
TICKERS = {
    "LQDT":  ("2022-01-01", "fund", "TQTF"),
    "GOLD":  ("2022-07-01", "fund", "TQTF"),
    "OBLG":  ("2022-12-09", "fund", "TQTF"),
    "EQMX":  ("2022-01-01", "fund", "TQTF"),
    "RVI":   ("2022-01-01", "index", "RTSI"),
    "IMOEX": ("2022-01-01", "index", "SNDX")  # Индекс МосБиржи
}

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

MAX_RETRIES = 5

# ============================================
# Настройка сессии для обхода защиты Мосбиржи
# ============================================
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
})


def fetch_moex_history_paginated(ticker, date_from, date_till, asset_type="fund", board="TQTF"):
    all_rows = []
    start = 0

    # Формируем правильный URL БЕЗ лишних пробелов
    if asset_type == "index":
        base_url = f"https://iss.moex.com/iss/history/engines/stock/markets/index/boards/{board}/securities/{ticker}.xml"
    else:
        base_url = f"https://iss.moex.com/iss/history/engines/stock/markets/shares/boards/{board}/securities/{ticker}.xml"

    while True:
        url = f"{base_url}?from={date_from}&till={date_till}&start={start}"
        print(f"🔹 Запрос: {url}")

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                # Экспоненциальная задержка перед повторной попыткой
                if attempt > 1:
                    delay = min(2 ** (attempt - 1) + random.uniform(0, 1), 10)
                    print(f"  ⏳ Пауза {delay:.1f} сек перед попыткой {attempt}")
                    time.sleep(delay)

                # Используем сессию вместо requests.get()
                r = session.get(url, timeout=(30, 60))  # connect=30s, read=60s
                r.raise_for_status()
                
                # Проверка на пустой ответ или ошибку в XML
                if not r.text.strip():
                    raise Exception("Пустой ответ от сервера")
                if "<error>" in r.text.lower():
                    raise Exception(f"Ошибка в ответе: {r.text[:300]}")
                
                root = ET.fromstring(r.text)
                break
                
            except requests.exceptions.Timeout as e:
                print(f"⚠ Таймаут (попытка {attempt}/{MAX_RETRIES}): {e}")
                if attempt == MAX_RETRIES:
                    print(f"❌ Пропускаем {ticker} (start={start}) после {MAX_RETRIES} попыток")
                    return pd.DataFrame()
                    
            except requests.exceptions.RequestException as e:
                print(f"⚠ Ошибка запроса (попытка {attempt}/{MAX_RETRIES}): {e}")
                if attempt == MAX_RETRIES:
                    print(f"❌ Пропускаем {ticker} (start={start}) после {MAX_RETRIES} попыток")
                    return pd.DataFrame()
                    
            except Exception as e:
                print(f"⚠ Неизвестная ошибка (попытка {attempt}/{MAX_RETRIES}): {e}")
                if attempt == MAX_RETRIES:
                    print(f"❌ Пропускаем {ticker} (start={start}) после {MAX_RETRIES} попыток")
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
        # Небольшая пауза между страницами для соблюдения рейт-лимитов
        time.sleep(0.5 + random.uniform(0, 0.5))

    if not all_rows:
        return pd.DataFrame()
    
    df = pd.DataFrame(all_rows)
    required_cols = ["TRADEDATE", "OPEN", "HIGH", "LOW", "CLOSE"]
    
    # Добавляем VOLUME если есть, иначе заполняем нулями
    if "VOLUME" not in df.columns:
        df["VOLUME"] = 0
    
    # Обработка пустых значений в числовых полях
    for col in ["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Оставляем только нужные колонки
    cols = [col for col in required_cols + ["VOLUME"] if col in df.columns]
    df = df[cols]
    df['TRADEDATE'] = pd.to_datetime(df['TRADEDATE'])
    return df.sort_values("TRADEDATE").reset_index(drop=True)


def update_ticker(ticker, start_date, asset_type, board):
    file_path = os.path.join(DATA_DIR, f"{ticker}.csv")

    if os.path.exists(file_path):
        df_old = pd.read_csv(file_path)
        df_old['TRADEDATE'] = pd.to_datetime(df_old['TRADEDATE'])
        last_date = (df_old['TRADEDATE'].max() + timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"📅 Последняя дата в {file_path}: {last_date} (запрашиваем с этой даты)")
    else:
        df_old = pd.DataFrame()
        last_date = start_date
        print(f"🆕 Файл не существует, начинаем с {start_date}")

    today = datetime.today().strftime("%Y-%m-%d")
    df_new = fetch_moex_history_paginated(ticker, last_date, today, asset_type, board)

    if df_new.empty:
        print(f"⚠ Нет новых данных для {ticker}")
        return

    if not df_old.empty:
        df_full = pd.concat([df_old, df_new]).drop_duplicates(subset="TRADEDATE").sort_values("TRADEDATE")
    else:
        df_full = df_new

    df_full.to_csv(file_path, index=False)
    print(f"✅ Обновлено: {file_path} — {len(df_new)} новых строк (всего {len(df_full)})")


if __name__ == "__main__":
    print(f"🚀 Запуск загрузки данных на {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🌐 Используем сессию с браузерными заголовками для обхода защиты Мосбиржи\n")
    
    for ticker, (start_date, asset_type, board) in TICKERS.items():
        print(f"\n{'='*60}")
        print(f"=== Обрабатываем {ticker} ({asset_type}, board={board}) ===")
        print(f"{'='*60}")
        update_ticker(ticker, start_date, asset_type, board)
        # Пауза между тикерами для соблюдения рейт-лимитов
        if ticker != list(TICKERS.keys())[-1]:
            delay = 1.5 + random.uniform(0, 0.5)
            print(f"⏳ Пауза {delay:.1f} сек между тикерами...")
            time.sleep(delay)
    
    print(f"\n🏁 Завершено в {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    session.close()  # Закрываем сессию
