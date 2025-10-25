import os
import pandas as pd
import numpy as np
import requests

# --------------- Параметры ---------------
DATA_DIR = "data/"
ASSETS = ["GOLD", "EQMX", "OBLG", "LQDT"]
RISK_FREE = "LQDT"
RISK_ASSETS = ["GOLD", "EQMX", "OBLG"]

# --- Пороги ---
RVI_THRESHOLD = 30
RSI_OVERBOUGHT = 70
VOLUME_RATIO_THRESHOLD = 0.8  # текущий объём >= 80% от 10-дневного среднего

# --- Telegram ---
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TELEGRAM_ENABLED = bool(BOT_TOKEN and CHAT_ID)


def compute_rsi(series, window=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).fillna(0)
    loss = (-delta.where(delta < 0, 0)).fillna(0)
    avg_gain = gain.rolling(window=window, min_periods=1).mean()
    avg_loss = loss.rolling(window=window, min_periods=1).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def load_and_prepare_data():
    dfs = {}
    # Загрузка и подготовка активов
    for asset in ASSETS:
        path = os.path.join(DATA_DIR, f"{asset}.csv")
        if not os.path.exists(path):
            raise FileNotFoundError(f"Файл не найден: {path}")
        df = pd.read_csv(path)
        df['Date'] = pd.to_datetime(df['TRADEDATE'], errors='coerce')
        df = df[['Date', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']].copy()

        for col in ['OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']:
            if col in df.columns:
                df[col] = (
                    df[col].astype(str)
                    .str.replace(' ', '', regex=False)
                    .str.replace(',', '.', regex=False)
                    .replace(['', '-', 'nan', 'None'], np.nan)
                )
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 🔥 Сразу переименовываем колонки под актив
        df = df.rename(columns={
            'OPEN': f'OPEN_{asset}',
            'HIGH': f'HIGH_{asset}',
            'LOW': f'LOW_{asset}',
            'CLOSE': f'CLOSE_{asset}',
            'VOLUME': f'VOLUME_{asset}'
        })
        dfs[asset] = df

    # Загрузка RVI
    rvi_path = os.path.join(DATA_DIR, "RVI.csv")
    if not os.path.exists(rvi_path):
        raise FileNotFoundError(f"Файл не найден: {rvi_path}")
    df_rvi = pd.read_csv(rvi_path)
    df_rvi['Date'] = pd.to_datetime(df_rvi['TRADEDATE'], errors='coerce')
    df_rvi = df_rvi[['Date', 'CLOSE']].rename(columns={'CLOSE': 'Close_RVI'})
    df_rvi['Close_RVI'] = pd.to_numeric(df_rvi['Close_RVI'], errors='coerce')
    dfs['RVI'] = df_rvi

    # Объединение данных
    df_merged = dfs[ASSETS[0]].copy()
    for asset in ASSETS[1:]:
        df_merged = df_merged.merge(dfs[asset], on='Date', how='inner')
    df_merged = df_merged.merge(dfs['RVI'], on='Date', how='inner')

    df_merged = df_merged.sort_values('Date').reset_index(drop=True)
    df_merged.replace([np.inf, -np.inf], np.nan, inplace=True)
    df_merged.dropna(inplace=True)
    return df_merged


def send_telegram_message(text: str):
    if not TELEGRAM_ENABLED:
        print("📤 Telegram не настроен (проверьте секреты)")
        return False
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"}
        response = requests.post(url, data=payload, timeout=10)
        if response.status_code == 200:
            print("✅ Сообщение отправлено в Telegram")
            return True
        else:
            print(f"❌ Ошибка Telegram API: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Исключение в Telegram: {e}")
        return False


def get_and_send_signal():
    print("🔍 Загрузка данных...")
    df = load_and_prepare_data()
    if len(df) < 200:
        msg = f"❌ Недостаточно данных ({len(df)} строк). Нужно ≥200 для индикаторов."
        print(msg)
        send_telegram_message(msg)
        return

    df = df.set_index('Date').sort_index()
    last_date = df.index[-1]
    current_rvi = df['Close_RVI'].iloc[-1]

    # --- Адаптивный LOOKBACK ---
    if current_rvi > 35:
        LOOKBACK = 10
    elif current_rvi > 25:
        LOOKBACK = 21
    else:
        LOOKBACK = 42

    if len(df) < LOOKBACK + 1:
        msg = f"❌ Недостаточно данных для LOOKBACK={LOOKBACK}"
        print(msg)
        send_telegram_message(msg)
        return

    # --- Расчёт моментума ---
    mom = {}
    for asset in RISK_ASSETS + [RISK_FREE]:
        price_today = df[f'CLOSE_{asset}'].iloc[-1]
        price_past = df[f'CLOSE_{asset}'].iloc[-(LOOKBACK + 1)]
        mom[asset] = price_today / price_past - 1

    # --- Расчёт индикаторов ---
    for asset in RISK_ASSETS:
        df[f'MA50_{asset}'] = df[f'CLOSE_{asset}'].rolling(50).mean()
        df[f'RSI_{asset}'] = compute_rsi(df[f'CLOSE_{asset}'], 14)
        df[f'VOL_MA10_{asset}'] = df[f'VOLUME_{asset}'].rolling(10).mean()

    # --- Применение фильтров ---
    filters = {asset: {"MA50": False, "RSI": False, "VOLUME": False} for asset in RISK_ASSETS}
    eligible = []

    for asset in RISK_ASSETS:
        price = df[f'CLOSE_{asset}'].iloc[-1]
        ma50 = df[f'MA50_{asset}'].iloc[-1]
        rsi = df[f'RSI_{asset}'].iloc[-1]
        vol_today = df[f'VOLUME_{asset}'].iloc[-1]
        vol_ma10 = df[f'VOL_MA10_{asset}'].iloc[-1]

        ma_ok = price > ma50
        rsi_ok = rsi < RSI_OVERBOUGHT
        vol_ok = vol_today >= vol_ma10 * VOLUME_RATIO_THRESHOLD

        filters[asset]["MA50"] = ma_ok
        filters[asset]["RSI"] = rsi_ok
        filters[asset]["VOLUME"] = vol_ok

        if ma_ok and rsi_ok and vol_ok:
            eligible.append(asset)

    # --- Выбор актива ---
    if current_rvi > RVI_THRESHOLD:
        selected = RISK_FREE
        rvi_note = f"⚠️ RVI = {current_rvi:.2f} > {RVI_THRESHOLD} → вход запрещён"
    else:
        if eligible:
            best_risk = max(eligible, key=lambda x: mom[x])
            selected = best_risk if mom[best_risk] > mom[RISK_FREE] else RISK_FREE
        else:
            selected = RISK_FREE
        rvi_note = f"✅ RVI = {current_rvi:.2f} ≤ {RVI_THRESHOLD}"

    # --- Формирование сообщения ---
    msg_lines = [
        f"📊 *Dual Momentum+ Signal*",
        f"Дата: {last_date.strftime('%Y-%m-%d')}",
        f"LOOKBACK: {LOOKBACK} дн. (адаптивно по RVI)",
        f"Рекомендация: *{selected}*",
        rvi_note,
        "",
        f"*Моментум ({LOOKBACK} дн.):*"
    ]

    for a in RISK_ASSETS + [RISK_FREE]:
        sign = "🟢" if a == selected else "⚪️"
        msg_lines.append(f"{sign} {a}: {mom[a]:+.2%}")

    msg_lines.append("")
    msg_lines.append("*Фильтры:*")
    for asset in RISK_ASSETS:
        ma_status = "✅" if filters[asset]["MA50"] else "❌"
        rsi_status = "✅" if filters[asset]["RSI"] else "⚠️"
        vol_status = "✅" if filters[asset]["VOLUME"] else "⚠️"
        msg_lines.append(f"{asset}: MA50={ma_status}, RSI={rsi_status}, VOL={vol_status}")

    message = "\n".join(msg_lines)

    print("\n" + "=" * 50)
    print("📤 Отправляемое сообщение:")
    print(message)
    print("=" * 50 + "\n")

    send_telegram_message(message)


if __name__ == "__main__":
    try:
        get_and_send_signal()
    except Exception as e:
        error_msg = f"❌ КРИТИЧЕСКАЯ ОШИБКА:\n{str(e)}"
        print(error_msg)
        send_telegram_message(error_msg)
        raise
