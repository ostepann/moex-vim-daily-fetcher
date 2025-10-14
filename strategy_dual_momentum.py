import os
import pandas as pd
import numpy as np

# --- Параметры ---
DATA_DIR = "./data"  # путь к папке с CSV
FILES = ["GOLD.csv", "EQMX.csv", "OBLG.csv", "LQDT.csv"]
ASSETS = ["GOLD", "EQMX", "OBLG", "LQDT"]
MULTIPLIERS = [0.025, 1, 1, 100]  # множители для цен
NUM_SHARES = [0, 0, 0, 613]       # если нужны

LOOKBACK = 126        # ~6 месяцев для momentuma
REBALANCE_FREQ = "W-SUN"  # еженедельно по воскресеньям

def load_and_prepare_data():
    dfs = {}
    for i, (file, asset) in enumerate(zip(FILES, ASSETS)):
        path = os.path.join(DATA_DIR, file)
        df = pd.read_csv(path, sep=',')
        
        # Приведение колонок к единому виду
        df.rename(columns=lambda x: x.strip(), inplace=True)
        df['Date'] = pd.to_datetime(df['TRADEDATE'], format='%Y-%m-%d', errors='coerce')
        df = df.sort_values('Date').reset_index(drop=True)
        
        # Очистка чисел и умножение на множитель
        for col in ['OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']:
            if col in df.columns:
                df[col] = (
                    df[col].astype(str)
                    .str.replace(' ', '', regex=False)
                    .str.replace(',', '.', regex=False)
                    .str.replace(r'[^0-9.]', '', regex=True)
                )
                df[col] = pd.to_numeric(df[col], errors='coerce')
                if col in ['OPEN', 'HIGH', 'LOW', 'CLOSE']:
                    df[col] = df[col] * MULTIPLIERS[i]
                df.rename(columns={col: f"{col.capitalize()}_{asset}"}, inplace=True)
        
        dfs[asset] = df[['Date'] + [c for c in df.columns if c != 'Date']]

    # Объединяем все по дате
    df_merged = dfs[ASSETS[0]][['Date']].copy()
    for asset in ASSETS:
        df_merged = df_merged.merge(dfs[asset], on='Date', how='inner')
    
    df_merged.replace([np.inf, -np.inf], np.nan, inplace=True)
    df_merged.dropna(inplace=True)
    df_merged.reset_index(drop=True, inplace=True)
    return df_merged

def add_indicators(df):
    for asset in ASSETS:
        close = f'Close_{asset}'
        high = f'High_{asset}'
        low = f'Low_{asset}'
        volume = f'Volume_{asset}'

        # Доходность
        df[f'return_{asset}'] = df[close].pct_change()

        # SMA
        df[f'SMA10_{asset}'] = df[close].rolling(10).mean()
        df[f'SMA30_{asset}'] = df[close].rolling(30).mean()

        # RSI
        delta = df[close].diff()
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = np.where(loss != 0, gain / loss, np.inf)
        df[f'RSI_{asset}'] = 100 - (100 / (1 + rs))

        # MACD
        ema12 = df[close].ewm(span=12).mean()
        ema26 = df[close].ewm(span=26).mean()
        df[f'MACD_{asset}'] = ema12 - ema26
        df[f'MACD_signal_{asset}'] = df[f'MACD_{asset}'].ewm(span=9).mean()
        df[f'MACD_hist_{asset}'] = df[f'MACD_{asset}'] - df[f'MACD_signal_{asset}']

        # Волатильность
        df[f'volatility_{asset}'] = df[f'return_{asset}'].rolling(10).std()

        # ATR
        tr1 = df[high] - df[low]
        tr2 = abs(df[high] - df[close].shift())
        tr3 = abs(df[low] - df[close].shift())
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        df[f'ATR_{asset}'] = tr.rolling(14).mean()

    df.dropna(inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df

def run_strategy():
    df = load_and_prepare_data()
    df = add_indicators(df)

    # Настройки стратегии
    risk_assets = ['GOLD', 'EQMX', 'OBLG']
    risk_free = 'LQDT'

    # Моментум
    for asset in risk_assets + [risk_free]:
        df[f'mom_{asset}'] = df[f'Close_{asset}'] / df[f'Close_{asset}'].shift(LOOKBACK) - 1

    # Даты ребалансировки
    df_reb = df.resample(REBALANCE_FREQ, on='Date').last()
    df_reb.dropna(subset=[f'mom_{risk_free}'], inplace=True)

    # Сигналы
    signals = pd.DataFrame(index=df_reb.index)
    for asset in risk_assets:
        signals[f'abs_{asset}'] = df_reb[f'mom_{asset}'] > df_reb[f'mom_{risk_free}']
        signals[f'mom_{asset}'] = df_reb[f'mom_{asset}']

    signals['best_asset'] = signals[[f'mom_{a}' for a in risk_assets]].idxmax(axis=1)
    signals['best_asset'] = signals['best_asset'].str.replace('mom_', '')

    # Фильтр абсолютного моментума
    def choose_asset(row):
        return row.best_asset if row[f'abs_{row.best_asset}'] else risk_free
    signals['selected'] = signals.apply(choose_asset, axis=1)

    # Симуляция доходности
    portfolio = pd.DataFrame(index=df_reb.index)
    portfolio['selected'] = signals['selected']

    rets = [np.nan]
    for i in range(1, len(df_reb)):
        prev_asset = portfolio['selected'].iloc[i-1]
        rets.append(df_reb.loc[df_reb.index[i], f'return_{prev_asset}'])
    portfolio['ret'] = rets
    portfolio['cum_ret'] = (1 + portfolio['ret'].fillna(0)).cumprod()

    # Сохраняем CSV
    csv_path = "dual_momentum_portfolio.csv"
    portfolio.to_csv(csv_path, index=True)

    return portfolio['cum_ret'].iloc[-1], csv_path

if __name__ == "__main__":
    cum_ret, csv_path = run_strategy()
    print(f"✅ Кумулятивная доходность стратегии: {cum_ret:.2%}")
    print(f"CSV сохранен в: {csv_path}")
