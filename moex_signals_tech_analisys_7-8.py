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

# Расширенные периоды для momentum
MOMENTUM_PERIODS = [2, 5, 10, 20]  # Добавлены 2 и 20 дня
PRICE_DYNAMICS = [1, 5, 10]  # Для ТА остаётся как есть
EMA_TREND_WINDOW = 5
MAX_STOP_DISTANCE = 0.03

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

def calculate_momentum(df, days):
    """Рассчитывает доходность за N торговых дней"""
    if len(df) <= days:
        return None
    current = df['close'].iloc[-1]
    past = df['close'].iloc[-(days + 1)]
    return (current - past) / past * 100

def get_dm_period_by_rvi(rvi_value):
    """Возвращает период Dual Momentum по RVI"""
    if rvi_value < 15:
        return 20
    elif rvi_value < 25:
        return 10
    else:  # RVI >= 25
        return 2

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
# Генерация сигнала ТА
# —————————————————————————————————————————————————————————————————————————————————————————————————————

def generate_ta_signal(ticker):
    """Ваша существующая функция ТА с небольшими правками"""
    df = load_csv(DAILY_PATHS[ticker])
    current_price = df['close'].iloc[-1]
    current_volume = df['volume'].iloc[-1]

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

    if current_rsi < 30:
        rsi_comment = "RSI: зона перепроданности → возможен отскок"
    elif current_rsi > 70:
        rsi_comment = "RSI: зона перекупленности → возможен откат"
    else:
        rsi_comment = ""

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
# Основная логика Dual Momentum + ТА
# —————————————————————————————————————————————————————————————————————————————————————————————————————

def main():
    from datetime import datetime, timezone
    dt = datetime.now(timezone.utc).astimezone().strftime("%d.%m.%Y %H:%M")
    
    # Получаем RVI
    try:
        rvi = get_latest_rvi()
    except:
        rvi = float('nan')
    
    # Определяем период DM
    dm_period = get_dm_period_by_rvi(rvi) if not pd.isna(rvi) else 20
    
    # Шкала RVI
    rvi_scale = "[ RVI<15 → 20д | 15≤RVI<25 → 10д | RVI≥25 → 2д ]"
    rvi_msg = f"RVI: {rvi:.1f} → DM-период: {dm_period} дня\n{rvi_scale}" if not pd.isna(rvi) else "RVI: N/A"

    # Рассчитываем momentum для всех активов
    momentum_data = {}
    risk_free_return = None
    
    for ticker in ["OBLG", "EQMX", "GOLD", "LQDT"]:
        try:
            df = load_csv(DAILY_PATHS[ticker])
            mom = {}
            for days in MOMENTUM_PERIODS:
                mom[days] = calculate_momentum(df, days)
            momentum_data[ticker] = mom
            
            if ticker == "LQDT":
                risk_free_return = mom.get(20, 0)  # 20 дней как proxy для годовой
        except Exception as e:
            momentum_data[ticker] = {d: None for d in MOMENTUM_PERIODS}

    # Формируем строку momentum
    mom_lines = []
    for ticker in ["OBLG", "EQMX", "GOLD"]:
        mom = momentum_data[ticker]
        parts = []
        for days in MOMENTUM_PERIODS:
            val = mom.get(days)
            if val is not None:
                sign = "+" if val >= 0 else ""
                parts.append(f"{sign}{val:.1f}% ({days}д)")
            else:
                parts.append(f"N/A ({days}д)")
        mom_lines.append(f"   {ticker}: {', '.join(parts)}")
    
    momentum_msg = "\n".join(mom_lines)

    # Выбираем лучший актив по DM-периоду
    candidates = ["OBLG", "EQMX", "GOLD"]
    best_ticker = None
    best_mom = -float('inf')
    
    for ticker in candidates:
        mom_val = momentum_data[ticker].get(dm_period)
        if mom_val is not None and mom_val > best_mom:
            best_mom = mom_val
            best_ticker = ticker

    # Генерируем ТА-сигнал для лучшего актива
    ta_result = None
    if best_ticker:
        try:
            ta_result = generate_ta_signal(best_ticker)
        except Exception as e:
            ta_result = None

    # Формируем финальное сообщение
    message = f"📊 *Комплексный сигнал на {dt} (MSK)*\n\n"
    message += f"{rvi_msg}\n\n"
    message += "📈 *Momentum (доходность)*:\n"
    message += f"{momentum_msg}\n\n"

    # Рекомендация
    if best_ticker and ta_result and ta_result["signal"] == "BUY":
        message += f"✅ *Рекомендуемый актив: {best_ticker}*\n"
        message += f"   - Лучший momentum за {dm_period} дня ({best_mom:+.1f}%)\n"
        message += f"   - ТА: {ta_result['interpretation']}\n"
        if ta_result["rsi_comment"]:
            message += f"   - {ta_result['rsi_comment']}\n"
        if ta_result["stop_loss"] or ta_result["take_profit"]:
            sl = f" Стоп: {ta_result['stop_loss']:.2f}" if ta_result["stop_loss"] else ""
            tp = f" Тейк: {ta_result['take_profit']:.2f}" if ta_result["take_profit"] else ""
            message += f"   →{sl}{tp}\n"
    else:
        message += "⚠️ *Рекомендация: LQDT*\n"
        message += f"   - Нет чёткого сигнала на рисковых активах\n"
        if best_ticker:
            reason = "ТА не подтверждает вход" if ta_result else "Ошибка расчёта ТА"
            message += f"   - {best_ticker}: лучший по momentum, но {reason}\n"

    # Отправка
    send_telegram(message.strip())

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

if __name__ == "__main__":
    main()
