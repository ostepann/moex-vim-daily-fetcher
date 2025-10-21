import os
import pandas as pd
import numpy as np
import requests

# --------------- Параметры ---------------
DATA_DIR = "data/"
FILES = ["GOLD.csv", "EQMX.csv", "OBLG.csv", "LQDT.csv", "RVI.csv"]
ASSETS = ["GOLD", "EQMX", "OBLG", "LQDT"]
MULTIPLIERS = [50, 1, 1, 100]  # GOLD: USD → RUB (~50), LQDT: копейки → рубли
LOOKBACK = 2 #126  # ~6 месяцев (торговых дней)
RVI_THRESHOLD = 30

# --- Telegram ---
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TELEGRAM_ENABLED = bool(BOT_TOKEN and CHAT_ID)


def load_and_prepare_data():
    dfs = {}
    # Загрузка основных активов
    for i, (file, asset) in enumerate(zip(FILES[:-1], ASSETS)):
        path = os.path.join(DATA_DIR, file)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Файл не найден: {path}")
        df = pd.read_csv(path)
        df['Date'] = pd.to_datetime(df['TRADEDATE'], errors='coerce')
        df = df.drop(columns=['TRADEDATE'], errors='ignore')
        df = df.sort_values('Date').reset_index(drop=True)

        for col in ['OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']:
            if col in df.columns:
                df[col] = (
                    df[col].astype(str)
                    .str.replace(' ', '', regex=False)
                    .str.replace(',', '.', regex=False)
                    .replace(['', '-', 'nan', 'None'], np.nan)
                )
                df[col] = pd.to_numeric(df[col], errors='coerce')
                if col in ['OPEN', 'HIGH', 'LOW', 'CLOSE']:
                    df[col] = df[col] * MULTIPLIERS[i]
                df.rename(columns={col: f"{col.capitalize()}_{asset}"}, inplace=True)

        dfs[asset] = df[['Date'] + [c for c in df.columns if c != 'Date']]

    # Загрузка RVI
    rvi_path = os.path.join(DATA_DIR, "RVI.csv")
    if not os.path.exists(rvi_path):
        raise FileNotFoundError(f"Файл не найден: {rvi_path}")
    df_rvi = pd.read_csv(rvi_path)
    df_rvi['Date'] = pd.to_datetime(df_rvi['TRADEDATE'], errors='coerce')
    df_rvi = df_rvi[['Date', 'CLOSE']].rename(columns={'CLOSE': 'Close_RVI'})
    df_rvi['Close_RVI'] = pd.to_numeric(df_rvi['Close_RVI'], errors='coerce')
    dfs['RVI'] = df_rvi

    # Объединение всех данных (только общие даты)
    df_merged = dfs[ASSETS[0]][['Date']].copy()
    for asset in ASSETS:
        df_merged = df_merged.merge(dfs[asset], on='Date', how='inner')
    df_merged = df_merged.merge(dfs['RVI'], on='Date', how='inner')

    df_merged = df_merged.sort_values('Date').reset_index(drop=True)
    df_merged.replace([np.inf, -np.inf], np.nan, inplace=True)
    df_merged.dropna(inplace=True)
    return df_merged


def send_telegram_message(text: str):
    if not TELEGRAM_ENABLED:
        print("📤 Telegram не настроен (проверьте TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID)")
        return False
    try:
        # 🔥 ВАЖНО: без пробелов в URL!
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": CHAT_ID,
            "text": text,
            "parse_mode": "Markdown"
        }
        response = requests.post(url, data=payload, timeout=10)
        if response.status_code == 200:
            print("✅ Сообщение отправлено в Telegram")
            return True
        else:
            print(f"❌ Ошибка Telegram API: {response.status_code} – {response.text}")
            return False
    except Exception as e:
        print(f"❌ Исключение при отправке в Telegram: {e}")
        return False


def get_and_send_signal():
    print("🔍 Загрузка и подготовка данных...")
    df = load_and_prepare_data()
    print(f"✅ Загружено {len(df)} строк общих данных")

    if len(df) < LOOKBACK + 1:
        msg = f"❌ Недостаточно данных для расчёта Dual Momentum (нужно ≥{LOOKBACK + 1} дней, есть {len(df)})"
        print(msg)
        send_telegram_message(msg)
        return

    df = df.set_index('Date').sort_index()
    risk_assets = ['GOLD', 'EQMX', 'OBLG']
    risk_free = 'LQDT'
    last_date = df.index[-1]
    current_rvi = df['Close_RVI'].iloc[-1]

    # Расчёт моментума
    mom = {}
    for asset in risk_assets + [risk_free]:
        price_today = df[f'Close_{asset}'].iloc[-1]
        price_past = df[f'Close_{asset}'].iloc[-(LOOKBACK + 1)]
        mom[asset] = price_today / price_past - 1

    # Логика выбора
    if current_rvi > RVI_THRESHOLD:
        selected = risk_free
        rvi_note = f"⚠️ RVI = {current_rvi:.2f} > {RVI_THRESHOLD} → вход в рисковые активы запрещён"
    else:
        best_risk = max(risk_assets, key=lambda x: mom[x])
        if mom[best_risk] > mom[risk_free]:
            selected = best_risk
        else:
            selected = risk_free
        rvi_note = f"✅ RVI = {current_rvi:.2f} ≤ {RVI_THRESHOLD} → фильтр пройден"

    # Формирование сообщения
    msg_lines = [
        f"📊 *Dual Momentum Signal*",
        f"Дата данных: {last_date.strftime('%Y-%m-%d')}",
        f"Рекомендация: вложить 100% в *{selected}*",
        rvi_note,
        "",
        f"*Моментум ({LOOKBACK} дн.):*"
    ]

    best_risk_overall = max(risk_assets, key=lambda x: mom[x])
    for a in risk_assets + [risk_free]:
        if a == selected:
            sign = "🟢"
        elif a == best_risk_overall:
            sign = "🔵"
        else:
            sign = "⚪️"
        msg_lines.append(f"{sign} {a}: {mom[a]:+.2%}")

    message = "\n".join(msg_lines)

    print("\n" + "=" * 50)
    print("📤 Отправляемое сообщение:")
    print(message)
    print("=" * 50 + "\n")

    send_telegram_message(message)


# --- Запуск с обработкой ошибок ---
if __name__ == "__main__":
    try:
        get_and_send_signal()
    except Exception as e:
        error_msg = f"❌ КРИТИЧЕСКАЯ ОШИБКА в Dual Momentum:\n{str(e)}"
        print(error_msg)
        send_telegram_message(error_msg)
        raise
