import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

DATA_DIR = "data"
ASSETS = ["GOLD", "EQMX", "OBLG"]
RISK_FREE = "LQDT"

def load_data():
    # Сигналы
    signals = pd.read_csv(os.path.join(DATA_DIR, "signals.csv"), parse_dates=["date"])
    signals = signals.set_index("date")["signal"]
    print(f"✅ Загружено {len(signals)} сигналов")
    
    # D1 для выхода
    d1 = {}
    for asset in ASSETS + [RISK_FREE]:
        df = pd.read_csv(os.path.join(DATA_DIR, f"{asset}.csv"), parse_dates=["TRADEDATE"])
        df = df.set_index("TRADEDATE")["CLOSE"].rename(asset)
        d1[asset] = df
    d1_full = pd.concat(d1.values(), axis=1).sort_index()
    
    # M1 данные (с правильным форматом)
    m1 = {}
    for asset in ASSETS:
        path = os.path.join(DATA_DIR, f"{asset}_M1_0959_1059.CSV")
        if not os.path.exists(path):
            raise FileNotFoundError(f"❌ Не найден M1-файл: {path}")
        df = pd.read_csv(path)
        df["begin"] = pd.to_datetime(df["begin"])
        df = df.set_index("begin").sort_index()
        m1[asset] = df
        print(f"✅ Загружено M1 для {asset}: {len(df)} свечей")
    
    return signals, d1_full, m1

def simulate_strategy(signals, d1_full, m1, min_return, window_minutes, fee=0.0004):
    portfolio = 1.0
    portfolio_values = []
    signal_days = signals.index.tolist()
    
    for i in range(len(signal_days) - 1):
        date = signal_days[i]
        next_date = signal_days[i + 1]
        if next_date not in d1_full.index:
            continue
        
        signal_asset = signals.loc[date]
        if signal_asset == RISK_FREE:
            ret = d1_full.loc[next_date, RISK_FREE] / d1_full.loc[date, RISK_FREE] - 1 - fee
            portfolio *= (1 + ret)
            portfolio_values.append((next_date, portfolio))
            continue
        
        trade_date = next_date
        m1_df = m1[signal_asset]
        # Фильтруем по дате (игнорируем время)
        m1_day = m1_df[m1_df.index.date == trade_date.date()]
        if len(m1_day) < window_minutes:
            # Нет данных → укрытие в LQDT
            ret = d1_full.loc[next_date, RISK_FREE] / d1_full.loc[date, RISK_FREE] - 1
            portfolio *= (1 + ret)
            portfolio_values.append((next_date, portfolio))
            continue
        
        open_price = m1_day.iloc[0]["open"]
        close_at_window = m1_day.iloc[window_minutes - 1]["close"]
        gain = close_at_window / open_price - 1
        
        if gain >= min_return:
            entry_price = close_at_window
            exit_price = d1_full.loc[next_date, signal_asset]
            ret = exit_price / entry_price - 1 - 2 * fee
            portfolio *= (1 + ret)
        else:
            ret = d1_full.loc[next_date, RISK_FREE] / d1_full.loc[date, RISK_FREE] - 1
            portfolio *= (1 + ret)
        
        portfolio_values.append((next_date, portfolio))
    
    if not portfolio_values:
        return pd.Series([1.0], index=[signals.index[0]])
    dates, values = zip(*portfolio_values)
    return pd.Series(values, index=dates)

def main():
    print("🔍 Загрузка данных...")
    signals, d1_full, m1 = load_data()
    
    min_returns = np.arange(0.0, 0.016, 0.001)  # 0.0% → 1.5%
    window_sizes = [5, 10, 15, 20, 25, 30]
    
    results = []
    best_return = -np.inf
    best_params = None
    best_series = None
    
    print(f"\n⚙️ Тестирование {len(min_returns) * len(window_sizes)} комбинаций...")
    for r in min_returns:
        for w in window_sizes:
            cumret = simulate_strategy(signals, d1_full, m1, r, w)
            total_ret = cumret.iloc[-1] - 1
            results.append((r, w, total_ret))
            if total_ret > best_return:
                best_return = total_ret
                best_params = (r, w)
                best_series = cumret
    
    # Базовая стратегия (всегда исполняем сигнал)
    base_cumret = simulate_strategy(signals, d1_full, m1, min_return=-1.0, window_minutes=1)
    base_return = base_cumret.iloc[-1] - 1
    
    best_r, best_w = best_params
    print(f"\n🏆 Лучший утренний фильтр:")
    print(f"   Мин. рост: {best_r*100:.2f}% за {best_w} минут")
    print(f"   Доходность: {best_return:.2%}")
    print(f"   Базовая:    {base_return:.2%}")
    print(f"   Дельта:     {best_return - base_return:+.2%}")
    
    # График
    plt.figure(figsize=(12, 6))
    plt.plot(base_cumret.index, base_cumret, label="Без фильтра", alpha=0.7)
    plt.plot(best_series.index, best_series, label=f"Фильтр: +{best_r*100:.2f}% за {best_w} мин", linewidth=2)
    plt.title("Оптимизация утреннего фильтра (60 дней)")
    plt.xlabel("Дата")
    plt.ylabel("Накопленная доходность")
    plt.legend()
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(DATA_DIR, "morning_filter_optimization.png"))
    plt.show()
    
    # Сохранение
    results_df = pd.DataFrame(results, columns=["min_return", "window_minutes", "total_return"])
    results_df.to_csv(os.path.join(DATA_DIR, "morning_filter_results.csv"), index=False)
    print(f"\n✅ Результаты сохранены.")

if __name__ == "__main__":
    main()
