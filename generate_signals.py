import os
import pandas as pd
from datetime import datetime

# === Настройки ===
DATA_DIR = "data"
ASSETS = ["GOLD", "EQMX", "OBLG"]
RISK_FREE = "LQDT"
LOOKBACK = 2  # lookback = 2 дня

# === Загрузка D1-данных ===
def load_d1_data():
    dfs = {}
    for asset in ASSETS + [RISK_FREE]:
        path = os.path.join(DATA_DIR, f"{asset}.csv")
        if not os.path.exists(path):
            raise FileNotFoundError(f"❌ Файл не найден: {path}")
        df = pd.read_csv(path, parse_dates=["TRADEDATE"])
        df = df.set_index("TRADEDATE")[["CLOSE"]].rename(columns={"CLOSE": asset})
        dfs[asset] = df
        print(f"✅ Загружен {asset}: {len(df)} строк")
    
    # Объединяем по дате (inner join — только общие дни)
    df = dfs[ASSETS[0]]
    for asset in ASSETS[1:] + [RISK_FREE]:
        df = df.merge(dfs[asset], left_index=True, right_index=True, how="inner")
    
    df = df.sort_index()
    print(f"📅 Общий период: {df.index.min()} — {df.index.max()} ({len(df)} дней)")
    return df

# === Генерация сигналов Dual Momentum ===
def generate_signals(df):
    signals = []
    dates = df.index.tolist()
    
    for i in range(LOOKBACK, len(df)):
        date = dates[i]
        # Расчёт momentum за последние 2 дня
        best_asset = RISK_FREE
        best_mom = -float("inf")
        
        rf_price_today = df[RISK_FREE].iloc[i]
        rf_price_past = df[RISK_FREE].iloc[i - LOOKBACK]
        rf_mom = rf_price_today / rf_price_past - 1
        
        for asset in ASSETS:
            price_today = df[asset].iloc[i]
            price_past = df[asset].iloc[i - LOOKBACK]
            mom = price_today / price_past - 1
            if mom > best_mom:
                best_mom = mom
                best_asset = asset
        
        # Выбираем рисковый актив, только если он лучше LQDT
        final_signal = best_asset if best_mom > rf_mom else RISK_FREE
        signals.append({"date": date, "signal": final_signal})
    
    return pd.DataFrame(signals)

# === Основной запуск ===
if __name__ == "__main__":
    print("🔍 Загрузка D1-данных...")
    df = load_d1_data()
    
    print(f"\n🎯 Генерация сигналов Dual Momentum (lookback={LOOKBACK})...")
    signals_df = generate_signals(df)
    
    output_path = os.path.join(DATA_DIR, "signals.csv")
    signals_df.to_csv(output_path, index=False)
    print(f"\n✅ Сохранено: {output_path}")
    print(f"📊 Пример последних сигналов:")
    print(signals_df.tail(5))
