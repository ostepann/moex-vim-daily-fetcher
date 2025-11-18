"""
Модуль генерации торговых сигналов по ETF MOEX (OBLG, EQMX, GOLD)
на основе дневных и часовых данных с учётом RVI и объёма.

Запуск: ежедневно в 11:30 по MSK.
"""

import os
import pandas as pd
import numpy as np
from scipy.signal import argrelextrema
import requests
from datetime import datetime, timedelta
import pytz


# --- Настройки ---
DATA_DIR = "/data"
TICKERS = ["OBLG", "EQMX", "GOLD"]
H1_SUFFIX = "_H1_35.CSV"
D1_SUFFIX = ".CSV"
RVI_FILE = "RVI.CSV"

# Telegram
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")


def load_daily_data(ticker):
    """Загрузка дневных данных из CSV."""
    file_path = os.path.join(DATA_DIR, ticker + D1_SUFFIX)
    df = pd.read_csv(file_path, sep="\t", parse_dates=["begin"])
    df.set_index("begin", inplace=True)
    df.sort_index(inplace=True)
    return df


def load_hourly_data(ticker):
    """Загрузка часовых данных из CSV."""
    file_path = os.path.join(DATA_DIR, ticker + H1_SUFFIX)
    df = pd.read_csv(file_path, sep="\t", parse_dates=["begin"])
    df.set_index("begin", inplace=True)
    df.sort_index(inplace=True)
    return df


def get_current_rvi():
    """Получение последнего значения RVI из файла."""
    file_path = os.path.join(DATA_DIR, RVI_FILE)
    df = pd.read_csv(file_path, sep="\t", parse_dates=["TRADEDATE"])
    df.set_index("TRADEDATE", inplace=True)
    df.sort_index(inplace=True)
    return df["CLOSE"].iloc[-1]


def adaptive_ema_span(rvi_value):
    """Определение окна EMA в зависимости от RVI."""
    if rvi_value > 25:
        return 20
    elif rvi_value > 15:
        return 35
    else:
        return 50


def find_levels(data, order=5):
    """Поиск сильных уровней поддержки и сопротивления."""
    lows = data['low'].values
    highs = data['high'].values

    min_idx = argrelextrema(lows, np.less, order=order)[0]
    max_idx = argrelextrema(highs, np.greater, order=order)[0]

    # Группируем близкие уровни
    tolerance = 0.1 if ticker in ["OBLG", "GOLD"] else 5.0

    support_levels = []
    for price in data['low'].iloc[min_idx].values:
        if not any(abs(price - lvl) <= tolerance for lvl in support_levels):
            support_levels.append(price)

    resistance_levels = []
    for price in data['high'].iloc[max_idx].values:
        if not any(abs(price - lvl) <= tolerance for lvl in resistance_levels):
            resistance_levels.append(price)

    return sorted(support_levels), sorted(resistance_levels)


def calculate_rsi(prices, period=14):
    """Расчёт RSI."""
    delta = prices.diff()
    gain = (delta.where(delta > 0, 0)).fillna(0)
    loss = (-delta.where(delta < 0, 0)).fillna(0)

    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def check_h1_confirmation(ticker, current_price, support_levels, resistance_levels):
    """Проверка подтверждения сигнала по часовым данным."""
    try:
        h1_data = load_hourly_data(ticker)
    except:
        return "hold"

    cutoff = datetime.now(pytz.timezone('Europe/Moscow')) - timedelta(hours=48)
    h1_recent = h1_data[h1_data.index >= cutoff]

    if len(h1_recent) < 10:
        return "hold"

    price = h1_recent['close'].iloc[-1]
    rsi_h1 = calculate_rsi(h1_recent['close']).iloc[-1]

    near_support = any(abs(price - lvl) / price < 0.005 for lvl in support_levels)
    near_resistance = any(abs(price - lvl) / price < 0.005 for lvl in resistance_levels)

    avg_vol = h1_recent['volume'].tail(5).mean()
    last_vol = h1_recent['volume'].iloc[-1]

    if near_support and rsi_h1 < 65 and last_vol > avg_vol * 0.8:
        return "buy"
    if near_resistance and rsi_h1 > 35 and last_vol > avg_vol * 0.8:
        return "sell"
    return "hold"


def generate_signal(ticker, rvi_value):
    """Генерация сигнала для одного тикера."""
    global current_ticker_for_tolerance
    current_ticker_for_tolerance = ticker

    try:
        daily_data = load_daily_data(ticker)
    except Exception as e:
        return {"status": "error", "message": str(e)}

    if len(daily_data) < 50:
        return {"status": "error", "message": "Недостаточно данных"}

    current_price = daily_data['close'].iloc[-1]
    ema_span = adaptive_ema_span(rvi_value)
    ema = daily_data['close'].ewm(span=ema_span, adjust=False).mean()
    current_ema = ema.iloc[-1]

    support, resistance = find_levels(daily_data, order=5)

    h1_signal = check_h1_confirmation(ticker, current_price, support, resistance)

    if h1_signal == "buy" and current_price > current_ema:
        final_signal = "BUY"
    elif h1_signal == "sell" and current_price < current_ema:
        final_signal = "SELL"
    else:
        final_signal = "HOLD"

    return {
        "status": "ok",
        "signal": final_signal,
        "price": round(current_price, 2),
        "ema_span": ema_span,
        "ema_value": round(current_ema, 2),
    }


def send_telegram(message):
    """Отправка сообщения в Telegram."""
    if not BOT_TOKEN or not CHAT_ID:
        print("BOT_TOKEN или CHAT_ID не заданы. Сообщение:", message)
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    try:
        requests.post(url, data=payload)
    except Exception as e:
        print(f"Ошибка отправки: {e}")


def main():
    # Получаем RVI
    try:
        rvi = get_current_rvi()
    except:
        rvi = 20.0

    moscow_tz = pytz.timezone('Europe/Moscow')
    now = datetime.now(moscow_tz)
    message = f"📊 *Сигналы на {now.strftime('%d.%m.%Y %H:%M')} (MSK)*\\nRVI: {rvi:.1f}\\n\\n"

    for ticker in TICKERS:
        sig = generate_signal(ticker, rvi)
        if sig["status"] != "ok":
            msg = f"🔴 {ticker}: ERROR ({sig['message']})\\n\\n"
        else:
            emoji = "🟢" if sig['signal'] == 'BUY' else "🔴" if sig['signal'] == 'SELL' else "🟡"
            msg = f"{emoji} {ticker}: *{sig['signal']}*\\n   Цена: {sig['price']}\\n   EMA({sig['ema_span']}): {sig['ema_value']}\\n\\n"
        message += msg

    send_telegram(message)


if __name__ == "__main__":
    main()
