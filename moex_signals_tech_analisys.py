import pandas as pd
import numpy as np
from scipy.signal import argrelextrema
import os
import requests

DAILY_PATHS = {
    "OBLG": "data/OBLG.csv",
    "EQMX": "data/EQMX.csv",
    "GOLD": "data/GOLD.csv",
}

HOURLY_PATHS = {
    "OBLG": "data/OBLG_H1_35.CSV",
    "EQMX": "data/EQMX_H1_35.CSV",
    "GOLD": "data/GOLD_H1_35.CSV",
}

RVI_PATH = "data/RVI.csv"

def load_csv(filepath):
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Файл не найден: {filepath}")
    df = pd.read_csv(filepath)
    print(f"\n🔍 Загрузка {filepath}")
    print(f"   Исходные колонки: {list(df.columns)}")
    df.columns = df.columns.str.lower()
    print(f"   Колонки после lower(): {list(df.columns)}")
    date_col = None
    for col in ['tradedate', 'begin']:
        if col in df.columns:
            date_col = col
            break
    if date_col:
        df[date_col] = pd.to_datetime(df[date_col])
        df.set_index(date_col, inplace=True)
    else:
        df.index = pd.to_datetime(df.index)
    print(f"   Дата-колонка: {date_col}")
    print(f"   Размер данных: {df.shape}")
    print(f"   Пример данных (последние 2 строки):\n{df.tail(2)}")
    print(f"   NaN в 'close': {df['close'].isna().sum()}")
    return df

def get_latest_rvi():
    df = load_csv(RVI_PATH)
    print(f"\n📊 RVI данные (последние 3 строки):\n{df[['close']].tail(3)}")
    return df['close'].iloc[-1]

def calculate_adaptive_ema_span(rvi_value):
    if rvi_value > 25:
        return 20
    elif rvi_value > 15:
        return 35
    else:
        return 50

def find_levels(data, order=5):
    if 'high' not in data.columns or 'low' not in data.columns:
        print(f"❌ В данных нет колонок 'high' или 'low'")
        return np.array([]), np.array([])
    highs = data['high'].values
    lows = data['low'].values
    min_idx = argrelextrema(lows, np.less, order=order)[0]
    max_idx = argrelextrema(highs, np.greater, order=order)[0]
    supports = lows[min_idx]
    resistances = highs[max_idx]

    def group_levels(levels):
        if len(levels) == 0:
            return np.array([])
        rounded = np.round(levels / 0.5) * 0.5
        counts = pd.Series(rounded).value_counts()
        strong_levels = counts[counts >= 2].index
        return strong_levels.values

    return group_levels(supports), group_levels(resistances)

def check_confirmation_h1(ticker):
    filepath = HOURLY_PATHS[ticker]
    if not os.path.exists(filepath):
        print(f"⚠️ H1 файл не найден: {filepath}")
        return True
    df_h1 = load_csv(filepath)
    if 'close' not in df_h1.columns:
        print(f"❌ В H1 данных нет 'close'")
        return True
    df_h1.sort_index(inplace=True)
    delta = df_h1['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    rsi_h1 = 100 - (100 / (1 + rs))
    current_rsi = rsi_h1.iloc[-1]
    print(f"   RSI(H1) для {ticker}: {current_rsi:.2f}")
    return 30 < current_rsi < 70

def generate_signal(ticker):
    df_daily = load_csv(DAILY_PATHS[ticker])
    df_daily.sort_index(inplace=True)

    if 'close' not in df_daily.columns or 'volume' not in df_daily.columns:
        print(f"❌ В дневных данных {ticker} нет нужных колонок")
        return "HOLD", "Ошибка данных", float('nan'), 50, float('nan'), float('nan'), [], [], float('nan')

    current_price = df_daily['close'].iloc[-1]
    current_volume = df_daily['volume'].iloc[-1]
    print(f"\n🎯 {ticker} — Цена: {current_price}, Объём: {current_volume}")

    try:
        rvi = get_latest_rvi()
    except Exception as e:
        print(f"❌ Ошибка RVI: {e}")
        rvi = float('nan')
    ema_span = calculate_adaptive_ema_span(rvi) if not pd.isna(rvi) else 50
    df_daily['ema'] = df_daily['close'].ewm(span=ema_span, adjust=False).mean()
    current_ema = df_daily['ema'].iloc[-1]
    print(f"   EMA({ema_span}): {current_ema}")

    supports, resistances = find_levels(df_daily)
    print(f"   Найдено уровней — Поддержка: {len(supports)}, Сопротивление: {len(resistances)}")

    nearby_supports = [level for level in supports if abs(current_price - level) / current_price < 0.02]
    nearby_resistances = [level for level in resistances if abs(current_price - level) / current_price < 0.02]
    print(f"   Ближайшие уровни — Поддержка: {nearby_supports}, Сопротивление: {nearby_resistances}")

    signal = "HOLD"
    reason = ""
    for level in nearby_supports:
        if current_price > current_ema and current_volume > df_daily['volume'].quantile(0.7):
            if check_confirmation_h1(ticker):
                signal = "BUY"
                reason = f"Поддержка: {level:.2f}, EMA({ema_span}): {current_ema:.2f}, объём ↑"
                break

    for level in nearby_resistances:
        if current_price < current_ema and current_volume > df_daily['volume'].quantile(0.7):
            if check_confirmation_h1(ticker):
                signal = "SELL"
                reason = f"Сопротивление: {level:.2f}, EMA({ema_span}): {current_ema:.2f}, объём ↑"
                break

    return signal, reason, rvi, ema_span, current_price, current_ema, nearby_supports, nearby_resistances, current_volume

def send_telegram(message):
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not bot_token or not chat_id:
        print("❌ TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID не заданы.")
        return
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message, "parse_mode": "Markdown"}
    try:
        response = requests.post(url, data=payload)
        if response.status_code == 200:
            print("✅ Сигнал отправлен в Telegram")
        else:
            print(f"❌ Ошибка Telegram: {response.text}")
    except Exception as e:
        print(f"❌ Ошибка отправки: {e}")

def main():
    from datetime import datetime, timezone
    dt = datetime.now(timezone.utc).astimezone().strftime("%d.%m.%Y %H:%M")
    message = f"📊 Сигналы на {dt} (MSK)\n"

    try:
        rvi = get_latest_rvi()
        message += f"RVI: {rvi:.1f}\n\n"
    except Exception as e:
        message += "RVI: N/A\n\n"
        rvi = float('nan')

    for ticker in ["OBLG", "EQMX", "GOLD"]:
        try:
            signal, reason, rvi_val, ema_span, price, ema_val, nearby_supports, nearby_resistances, volume = generate_signal(ticker)
            emoji = {"BUY": "🟢", "SELL": "🔴", "HOLD": "🟡"}.get(signal, "⚪")
            message += f"{emoji} *{ticker}*\n"
            message += f"   Цена: {price:.2f}\n"
            message += f"   EMA({ema_span}): {ema_val:.2f}\n"
            message += f"   Объём: {volume:.0f}\n"
            message += f"   Поддержки вблизи: [{', '.join([f'{x:.2f}' for x in sorted(nearby_supports)])}]\n"
            message += f"   Сопротивления вблизи: [{', '.join([f'{x:.2f}' for x in sorted(nearby_resistances)])}]\n"
            message += f"   Рекомендация: {signal}\n"
            if reason:
                message += f"   - {reason}\n"
            else:
                message += f"   - Нет чёткого сигнала\n"
            message += "\n"
        except Exception as e:
            message += f"🔴 {ticker}: ERROR ({str(e)})\n\n"

    send_telegram(message.strip())

if __name__ == "__main__":
    main()
