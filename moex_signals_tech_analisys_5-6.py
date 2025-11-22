import pandas as pd
import numpy as np
from scipy.signal import argrelextrema
import os
import requests

# —————————————————————————————————————————————————————————————————————————————————————————————————————
# Конфигурация
# —————————————————————————————————————————————————————————————————————————————————————————————————————

DAILY_PATHS = {
    "OBLG": "data/OBLG.csv",
    "EQMX": "data/EQMX.csv",
    "GOLD": "data/GOLD.csv",
    "LQDT": "data/LQDT.csv",
}

HOURLY_PATHS = {
    "OBLG": "data/OBLG_H1_35.CSV",
    "EQMX": "data/EQMX_H1_35.CSV",
    "GOLD": "data/GOLD_H1_35.CSV",
}

RVI_PATH = "data/RVI.csv"

PRICE_DYNAMICS = [1, 5, 10]
EMA_TREND_WINDOW = 5

# —————————————————————————————————————————————————————————————————————————————————————————————————————
# Загрузка и очистка данных
# —————————————————————————————————————————————————————————————————————————————————————————————————————

def load_csv(filepath):
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Файл не найден: {filepath}")
    df = pd.read_csv(filepath)
    df.columns = df.columns.str.lower()
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
    df = df.dropna()
    df = df[df.index.notna()]
    df.sort_index(inplace=True)
    return df

def get_latest_rvi():
    df = load_csv(RVI_PATH)
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
        return True
    df_h1 = load_csv(filepath)
    if 'close' not in df_h1.columns:
        return True
    df_h1.sort_index(inplace=True)
    delta = df_h1['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    rsi_h1 = 100 - (100 / (1 + rs))
    current_rsi = rsi_h1.iloc[-1]
    return 30 < current_rsi < 70

# —————————————————————————————————————————————————————————————————————————————————————————————————————
# Генерация сигнала
# —————————————————————————————————————————————————————————————————————————————————————————————————————

def generate_signal(ticker):
    df = load_csv(DAILY_PATHS[ticker])
    current_price = df['close'].iloc[-1]
    current_volume = df['volume'].iloc[-1]

    try:
        rvi = get_latest_rvi()
    except:
        rvi = float('nan')
    ema_span = calculate_adaptive_ema_span(rvi) if not pd.isna(rvi) else 50
    df['ema'] = df['close'].ewm(span=ema_span, adjust=False).mean()
    current_ema = df['ema'].iloc[-1]

    if len(df) >= EMA_TREND_WINDOW + 1:
        ema_prev = df['ema'].iloc[-EMA_TREND_WINDOW]
        ema_trend = "растёт" if current_ema > ema_prev else "падает"
    else:
        ema_trend = "недостаточно данных"

    price_changes = {}
    for days in PRICE_DYNAMICS:
        if len(df) > days:
            past_price = df['close'].iloc[-(days + 1)]
            change_pct = (current_price - past_price) / past_price * 100
            price_changes[days] = change_pct
        else:
            price_changes[days] = None

    volume_ratios = {}
    for days in PRICE_DYNAMICS:
        if len(df) > days and days >= 1:
            start_idx = -(days + 1)
            end_idx = -1
            if start_idx < -len(df):
                start_idx = 0
            vol_slice = df['volume'].iloc[start_idx:end_idx]
            if len(vol_slice) > 0:
                avg_vol = vol_slice.mean()
                if avg_vol > 0:
                    ratio = current_volume / avg_vol
                    volume_ratios[days] = ratio
                else:
                    volume_ratios[days] = 1.0
            else:
                volume_ratios[days] = 1.0
        else:
            volume_ratios[days] = 1.0

    def format_volume_ratios(ratios):
        parts = []
        for days in [1, 5, 10]:
            val = ratios.get(days, 1.0)
            if pd.isna(val) or val is None or not isinstance(val, (int, float)):
                val = 1.0
            parts.append(f"{val:.1f}x за {days} дн")
        return ", ".join(parts)

    volume_desc = format_volume_ratios(volume_ratios)

    # —————————————————————————————————————
    # ИЗМЕНЕНО: всегда показываем хотя бы 1 уровень
    # —————————————————————————————————————
    supports, resistances = find_levels(df)

    nearby_supports = [level for level in supports if abs(current_price - level) / current_price < 0.015]
    nearby_resistances = [level for level in resistances if abs(current_price - level) / current_price < 0.015]

    if not nearby_supports and len(supports) > 0:
        nearest_support = min(supports, key=lambda x: abs(current_price - x))
        nearby_supports = [nearest_support]

    if not nearby_resistances and len(resistances) > 0:
        nearest_resistance = min(resistances, key=lambda x: abs(current_price - x))
        nearby_resistances = [nearest_resistance]
    # —————————————————————————————————————

    signal = "HOLD"
    interpretation = ""

    if price_changes[1] is not None and price_changes[5] is not None:
        short_trend = "рост" if price_changes[1] > 0 else "падение"
    else:
        short_trend = "недостаточно данных"

    vol_5d = volume_ratios.get(5, 1.0)
    vol_ok = isinstance(vol_5d, (int, float)) and not pd.isna(vol_5d) and vol_5d > 1.5

    if nearby_supports and vol_ok and check_confirmation_h1(ticker):
        interpretation = f"Цена у поддержки, объём высокий → возможен отскок ({short_trend})"
        if current_price > current_ema:
            signal = "BUY"
    elif nearby_resistances and vol_ok and check_confirmation_h1(ticker):
        interpretation = f"Цена у сопротивления, объём высокий → возможен разворот ({short_trend})"
        if current_price < current_ema:
            signal = "SELL"
    elif vol_ok and current_price > current_ema and price_changes[5] and price_changes[5] > 0:
        interpretation = "Сильный восходящий тренд + высокий объём → продолжение роста"
        signal = "BUY"
    else:
        interpretation = "Нет чёткого сигнала"

    return {
        "ticker": ticker,
        "price": current_price,
        "price_changes": price_changes,
        "ema_span": ema_span,
        "ema_value": current_ema,
        "ema_trend": ema_trend,
        "volume_desc": volume_desc,
        "supports": sorted(nearby_supports),
        "resistances": sorted(nearby_resistances),
        "signal": signal,
        "interpretation": interpretation,
        "rvi": rvi
    }

# —————————————————————————————————————————————————————————————————————————————————————————————————————
# Вспомогательные функции
# —————————————————————————————————————————————————————————————————————————————————————————————————————

def format_price_changes(changes):
    parts = []
    for days in [1, 5, 10]:
        val = changes.get(days, None)
        if val is not None and isinstance(val, (int, float)) and not pd.isna(val):
            sign = "+" if val >= 0 else ""
            parts.append(f"{sign}{val:.1f}% за {days} дн")
        else:
            parts.append(f"N/A за {days} дн")
    return ", ".join(parts)

# —————————————————————————————————————————————————————————————————————————————————————————————————————
# Отправка в Telegram
# —————————————————————————————————————————————————————————————————————————————————————————————————————

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

# —————————————————————————————————————————————————————————————————————————————————————————————————————
# Основная функция
# —————————————————————————————————————————————————————————————————————————————————————————————————————

def main():
    from datetime import datetime, timezone
    dt = datetime.now(timezone.utc).astimezone().strftime("%d.%m.%Y %H:%M")
    
    try:
        rvi = get_latest_rvi()
        rvi_msg = f"RVI: {rvi:.1f} (высокая волатильность)" if rvi > 25 else f"RVI: {rvi:.1f}"
    except Exception as e:
        rvi_msg = "RVI: N/A"

    lqdt_dyn = ""
    try:
        df_lqdt = load_csv(DAILY_PATHS["LQDT"])
        current = df_lqdt['close'].iloc[-1]
        changes = {}
        for days in PRICE_DYNAMICS:
            if len(df_lqdt) > days:
                past = df_lqdt['close'].iloc[-(days + 1)]
                changes[days] = (current - past) / past * 100
            else:
                changes[days] = None
        lqdt_dyn = f"   LQDT: {current:.2f} ({format_price_changes(changes)})\n"
    except Exception as e:
        lqdt_dyn = "   LQDT: недоступен\n"

    message = f"📊 *Сигналы на {dt} (MSK)*\n{rvi_msg}\n{lqdt_dyn}\n"

    for ticker in ["OBLG", "EQMX", "GOLD"]:
        try:
            data = generate_signal(ticker)
            emoji = {"BUY": "🟢", "SELL": "🔴", "HOLD": "🟡"}.get(data["signal"], "⚪")
            price_changes_str = format_price_changes(data["price_changes"])
            message += f"{emoji} *{ticker}*\n"
            message += f"   Цена: {data['price']:.2f} ({price_changes_str})\n"
            message += f"   EMA({data['ema_span']}): {data['ema_value']:.2f} ({data['ema_trend']})\n"
            message += f"   Объём: {data['volume_desc']}\n"
            message += f"   Поддержки вблизи: [{', '.join([f'{x:.2f}' for x in data['supports']])}]\n"
            message += f"   Сопротивления вблизи: [{', '.join([f'{x:.2f}' for x in data['resistances']])}]\n"
            message += f"   Рекомендация: {data['signal']}\n"
            message += f"   - {data['interpretation']}\n"
            message += "\n"
        except Exception as e:
            message += f"🔴 {ticker}: ERROR ({str(e)})\n\n"

    send_telegram(message.strip())

if __name__ == "__main__":
    main()
