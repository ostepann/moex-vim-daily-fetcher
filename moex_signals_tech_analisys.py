import pandas as pd
import numpy as np
from scipy.signal import argrelextrema
import os
import requests
import sys

# —————————————————————————————————————————————————————————————————————————————————————————————————————
# Пути к файлам (из репо moex-vim-daily-fetcher)
# —————————————————————————————————————————————————————————————————————————————————————————————————————

DAILY_PATHS = {
    "OBLG": "data-repo/data/OBLG.csv",
    "EQMX": "data-repo/data/EQMX.csv",
    "GOLD": "data-repo/data/GOLD.csv",
}

HOURLY_PATHS = {
    "OBLG": "data-repo/data/OBLG_H1_35.CSV",
    "EQMX": "data-repo/data/EQMX_H1_35.CSV",
    "GOLD": "data-repo/data/GOLD_H1_35.CSV",
}

RVI_PATH = "data-repo/data/RVI.csv"

# —————————————————————————————————————————————————————————————————————————————————————————————————————
# Загрузка данных
# —————————————————————————————————————————————————————————————————————————————————————————————————————

def load_csv(filepath):
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Файл не найден: {filepath}")
    df = pd.read_csv(filepath)
    df['TRADEDATE'] = pd.to_datetime(df['TRADEDATE'])
    df.set_index('TRADEDATE', inplace=True)
    return df

def get_latest_rvi():
    df = load_csv(RVI_PATH)
    return df['CLOSE'].iloc[-1]

def calculate_adaptive_ema_span(rvi_value):
    if rvi_value > 25:
        return 20
    elif rvi_value > 15:
        return 35
    else:
        return 50

def find_levels(data, order=5):
    closes = data['close'].values
    highs = data['high'].values
    lows = data['low'].values

    # Локальные минимумы (уровни поддержки)
    min_idx = argrelextrema(lows, np.less, order=order)[0]
    support_levels = lows[min_idx]

    # Локальные максимумы (уровни сопротивления)
    max_idx = argrelextrema(highs, np.greater, order=order)[0]
    resistance_levels = highs[max_idx]

    # Группировка близких уровней (округление до 0.5)
    def group_levels(levels):
        rounded = np.round(levels / 0.5) * 0.5
        counts = pd.Series(rounded).value_counts()
        strong_levels = counts[counts >= 2].index
        return strong_levels.values

    strong_support = group_levels(support_levels)
    strong_resistance = group_levels(resistance_levels)

    return strong_support, strong_resistance

def check_confirmation_h1(ticker):
    filepath = HOURLY_PATHS[ticker]
    if not os.path.exists(filepath):
        print(f"⚠️ Часовые данные для {ticker} не найдены: {filepath}")
        return True  # Если H1 нет — пропускаем подтверждение

    df_h1 = load_csv(filepath)
    df_h1.sort_index(inplace=True)

    # RSI(14) на H1
    delta = df_h1['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    rsi_h1 = 100 - (100 / (1 + rs))

    current_rsi = rsi_h1.iloc[-1]
    current_price = df_h1['close'].iloc[-1]

    # Подтверждение: RSI в разумных пределах
    if current_rsi < 30 or current_rsi > 70:
        return False
    return True

def generate_signal(ticker):
    # Загрузка дневных данных
    df_daily = load_csv(DAILY_PATHS[ticker])
    df_daily.sort_index(inplace=True)

    # Последняя цена
    current_price = df_daily['close'].iloc[-1]
    current_volume = df_daily['volume'].iloc[-1]

    # RVI и адаптивная EMA
    rvi = get_latest_rvi()
    ema_span = calculate_adaptive_ema_span(rvi)
    df_daily['EMA'] = df_daily['close'].ewm(span=ema_span, adjust=False).mean()
    current_ema = df_daily['EMA'].iloc[-1]

    # Поиск уровней
    supports, resistances = find_levels(df_daily)

    # Определение сигнала
    signal = "HOLD"
    reason = ""

    # Проверка на близость к поддержке
    for level in supports:
        if abs(current_price - level) / current_price < 0.01:  # В пределах 1%
            if current_price > current_ema and current_volume > df_daily['volume'].quantile(0.7):
                if check_confirmation_h1(ticker):
                    signal = "BUY"
                    reason = f"Поддержка: {level:.2f}, EMA({ema_span}): {current_ema:.2f}, объём ↑"
                    break

    # Проверка на близость к сопротивлению
    for level in resistances:
        if abs(current_price - level) / current_price < 0.01:  # В пределах 1%
            if current_price < current_ema and current_volume > df_daily['volume'].quantile(0.7):
                if check_confirmation_h1(ticker):
                    signal = "SELL"
                    reason = f"Сопротивление: {level:.2f}, EMA({ema_span}): {current_ema:.2f}, объём ↑"
                    break

    return signal, reason, rvi

def send_telegram(message):
    bot_token = os.getenv("BOT_TOKEN")
    chat_id = os.getenv("CHAT_ID")

    if not bot_token:
        print("❌ BOT_TOKEN не задан в secrets")
        return
    if not chat_id:
        print("❌ CHAT_ID не задан в secrets")
        return

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown"
    }

    try:
        response = requests.post(url, data=payload)
        if response.status_code != 200:
            print(f"❌ Ошибка Telegram API: {response.text}")
        else:
            print("✅ Сигнал отправлен в Telegram")
    except Exception as e:
        print(f"❌ Ошибка отправки: {e}")

def main():
    rvi = get_latest_rvi()
    from datetime import datetime
    dt = datetime.now().strftime("%d.%m.%Y %H:%M")
    message = f"📊 *Сигналы на {dt} (MSK)*\nRVI: {rvi:.1f}\n\n"

    for ticker in ["OBLG", "EQMX", "GOLD"]:
        try:
            signal, reason, _ = generate_signal(ticker)
            emoji = {"BUY": "🟢", "SELL": "🔴", "HOLD": "🟡"}[signal]
            message += f"{emoji} {ticker}: {signal}\n"
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
