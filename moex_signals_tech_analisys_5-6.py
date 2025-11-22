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
MAX_STOP_DISTANCE = 0.03  # 3%

# —————————————————————————————————————————————————————————————————————————————————————————————————————
# Вспомогательные индикаторы
# —————————————————————————————————————————————————————————————————————————————————————————————————————

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

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
# Генерация сигнала (с RSI-интерпретацией)
# —————————————————————————————————————————————————————————————————————————————————————————————————————

def generate_signal(ticker):
    df = load_csv(DAILY_PATHS[ticker])
    current_price = df['close'].iloc[-1]
    current_volume = df['volume'].iloc[-1]

    # ——— RSI ———
    rsi_series = calculate_rsi(df['close'])
    current_rsi = rsi_series.iloc[-1] if len(rsi_series) > 0 else 50

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

    # Уровни
    supports_all, resistances_all = find_levels(df)
    supports_below = [s for s in supports_all if s < current_price]
    resistances_above = [r for r in resistances_all if r > current_price]

    supports_near = [s for s in supports_below if (current_price - s) / current_price < 0.015]
    resistances_near = [r for r in resistances_above if (r - current_price) / current_price < 0.015]

    if not supports_near and supports_below:
        nearest_support = max(supports_below)
        supports_near = [nearest_support]

    if not resistances_near and resistances_above:
        nearest_resistance = min(resistances_above)
        resistances_near = [nearest_resistance]

    h1_confirmed = check_confirmation_h1(ticker)

    signal = "HOLD"
    interpretation = "Нет чёткого сигнала"
    rsi_comment = ""
    stop_loss = None
    take_profit = None

    # ——— Интерпретация RSI ———
    if current_rsi < 30:
        rsi_comment = "RSI: зона перепроданности → возможен отскок"
    elif current_rsi > 70:
        rsi_comment = "RSI: зона перекупленности → возможен откат"
    else:
        rsi_comment = ""

    # ——— Правила сигналов ———
    if (ema_trend == "растёт" and current_price > current_ema and 
        price_changes[5] and price_changes[5] > 3 and volume_ratios[10] > 1.5 and h1_confirmed):
        signal = "BUY"
        interpretation = "Сильный восходящий тренд + высокий объём → продолжение роста"
        take_profit = resistances_above[0] if resistances_above else current_price * 1.02
        recent_supports = [s for s in supports_below if (current_price - s) / current_price <= 0.05]
        if recent_supports:
            stop_loss = max(recent_supports) * 0.995
        else:
            stop_loss = current_ema * 0.99

    elif (ema_trend == "растёт" and current_price > current_ema and 
          price_changes[1] and price_changes[1] < 0 and 
          price_changes[5] and price_changes[5] > 2 and h1_confirmed):
        signal = "HOLD"
        interpretation = "Коррекция в восходящем тренде. Ждём подтверждения отскока"

    elif (supports_near and current_price > supports_near[-1] * 0.995 and 
          volume_ratios[5] > 1.3 and h1_confirmed and
          current_price > current_ema):
        signal = "BUY"
        base_msg = "Цена у поддержки, объём высокий → возможен отскок вверх"
        interpretation = f"{base_msg}. {rsi_comment}" if rsi_comment else base_msg
        take_profit = resistances_above[0] if resistances_above else current_price * 1.015
        stop_loss = supports_near[-1] * 0.99

    elif (resistances_near and current_price > resistances_near[0] and 
          volume_ratios[1] > 1.5 and h1_confirmed and
          current_price > current_ema):
        signal = "BUY"
        interpretation = "Пробой сопротивления на высоком объёме → вход после подтверждения"
        take_profit = current_price * 1.02
        stop_loss = resistances_near[0] * 0.995

    elif (len(df) > 10 and 
          current_price > df['ema'].iloc[-5] and
          supports_near and current_price < supports_near[0] * 1.005 and
          volume_ratios[1] > 0.8 and h1_confirmed and
          current_price > current_ema):
        signal = "BUY"
        base_msg = "Тест бывшего сопротивления (теперь поддержка) → идеальная точка входа"
        interpretation = f"{base_msg}. {rsi_comment}" if rsi_comment else base_msg
        take_profit = resistances_above[0] if resistances_above else current_price * 1.02
        stop_loss = supports_near[0] * 0.99

    elif rvi > 25 and not h1_confirmed:
        signal = "HOLD"
        interpretation = f"Высокая волатильность (RVI={rvi:.1f}). Требуется подтверждение по H1"

    elif (ema_trend == "падает" and current_price < current_ema and 
          price_changes[5] and price_changes[5] < -3 and volume_ratios[10] > 1.5 and h1_confirmed):
        signal = "HOLD"
        interpretation = "Сильный нисходящий тренд. Избегать лонгов."

    # ——— Финальные гарантии ———
    if signal == "BUY":
        max_stop = current_price * (1 - MAX_STOP_DISTANCE)
        if stop_loss is None or stop_loss >= current_price:
            stop_loss = max_stop
        elif stop_loss < max_stop:
            stop_loss = max_stop
        if take_profit is None or take_profit <= current_price:
            take_profit = current_price * 1.015

    if stop_loss:
        stop_loss = round(stop_loss, 2)
    if take_profit:
        take_profit = round(take_profit, 2)

    return {
        "ticker": ticker,
        "price": current_price,
        "price_changes": price_changes,
        "ema_span": ema_span,
        "ema_value": current_ema,
        "ema_trend": ema_trend,
        "volume_desc": volume_desc,
        "supports": sorted(supports_near),
        "resistances": sorted(resistances_near),
        "signal": signal,
        "interpretation": interpretation,
        "rsi_comment": rsi_comment,
        "stop_loss": stop_loss,
        "take_profit": take_profit,
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
            if data["rsi_comment"]:
                message += f"   - {data['rsi_comment']}\n"
            if data["stop_loss"] or data["take_profit"]:
                sl = f" Стоп: {data['stop_loss']:.2f}" if data["stop_loss"] else ""
                tp = f" Тейк: {data['take_profit']:.2f}" if data["take_profit"] else ""
                message += f"   →{sl}{tp}\n"
            message += "\n"
        except Exception as e:
            message += f"🔴 {ticker}: ERROR ({str(e)})\n\n"

    send_telegram(message.strip())

if __name__ == "__main__":
    main()
