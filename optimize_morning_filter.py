import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

DATA_DIR = "data"
ASSETS = ["GOLD", "EQMX", "OBLG"]
RISK_FREE = "LQDT"


def load_data():
    # Загрузка сигналов
    signals = pd.read_csv(os.path.join(DATA_DIR, "signals.csv"), parse_dates=["date"])
    signals = signals.set_index("date")["signal"]
    print(f"✅ Загружено {len(signals)} сигналов")

    # Загрузка D1
    d1_parts = {}
    for asset in ASSETS + [RISK_FREE]:
        df = pd.read_csv(os.path.join(DATA_DIR, f"{asset}.csv"), parse_dates=["TRADEDATE"])
        df = df.set_index("TRADEDATE")["CLOSE"].rename(asset)
        d1_parts[asset] = df
    d1_full = pd.concat(d1_parts.values(), axis=1).sort_index()
    print(f"📅 D1 период: {d1_full.index.min()} — {d1_full.index.max()}")

    # Загрузка M1
    m1 = {}
    for asset in ASSETS:
        path = os.path.join(DATA_DIR, f"{asset}_M1_0959_1059.CSV")
        df = pd.read_csv(path)
        df["begin"] = pd.to_datetime(df["begin"])
        df = df.set_index("begin")
        m1[asset] = df
        print(f"✅ M1 для {asset}: {len(df)} строк")

    return signals, d1_full, m1


def simulate_strategy(signals, d1_full, m1, min_return, window_minutes, fee=0.0004):
    portfolio = 1.0
    portfolio_values = []

    trading_days = d1_full.index.tolist()
    if len(trading_days) < 2:
        return pd.Series([1.0], index=[trading_days[0]] if trading_days else [pd.Timestamp("2023-01-01")])

    for i in range(len(trading_days) - 1):
        date = trading_days[i]        # день D — сигнал
        next_date = trading_days[i + 1]  # день D+1 — вход/выход

        # Определяем актив на день D
        if date in signals.index:
            asset = signals.loc[date]
        else:
            asset = RISK_FREE

        if asset == RISK_FREE:
            ret = d1_full.loc[next_date, RISK_FREE] / d1_full.loc[date, RISK_FREE] - 1 - fee
            portfolio *= (1 + ret)
            portfolio_values.append((next_date, portfolio))
            continue

        # --- Работа с M1 на next_date ---
        target_date = next_date.date()
        m1_df = m1[asset]

        # Приводим индекс M1 к naive datetime (без таймзоны)
        if m1_df.index.tz is not None:
            m1_df = m1_df.tz_localize(None)

        # Фильтрация по дате
        m1_day = m1_df[m1_df.index.date == target_date]

        if len(m1_day) < window_minutes:
            # Нет данных → укрытие
            ret = d1_full.loc[next_date, RISK_FREE] / d1_full.loc[date, RISK_FREE] - 1
            portfolio *= (1 + ret)
            portfolio_values.append((next_date, portfolio))
            continue

        open_price = m1_day.iloc[0]["open"]
        close_at_window = m1_day.iloc[window_minutes - 1]["close"]
        gain = close_at_window / open_price - 1

        if gain >= min_return:
            entry_price = close_at_window
            exit_price = d1_full.loc[next_date, asset]
            ret = exit_price / entry_price - 1 - 2 * fee
            portfolio *= (1 + ret)
        else:
            ret = d1_full.loc[next_date, RISK_FREE] / d1_full.loc[date, RISK_FREE] - 1
            portfolio *= (1 + ret)

        portfolio_values.append((next_date, portfolio))

    if not portfolio_values:
        return pd.Series([1.0], index=[trading_days[0]])
    dates, values = zip(*portfolio_values)
    return pd.Series(values, index=dates)


def main():
    print("🔍 Загрузка данных...")
    signals, d1_full, m1 = load_data()

    # Параметры
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
            total_ret = cumret.iloc[-1] - 1 if len(cumret) > 0 else -1.0
            results.append((r, w, total_ret))
            if total_ret > best_return:
                best_return = total_ret
                best_params = (r, w)
                best_series = cumret

    # Базовая стратегия (без фильтра)
    base_cumret = simulate_strategy(signals, d1_full, m1, min_return=-1.0, window_minutes=1)
    base_return = base_cumret.iloc[-1] - 1 if len(base_cumret) > 0 else 0.0

    # === ГАРАНТИРОВАННОЕ СОЗДАНИЕ ФАЙЛОВ ===
    results_df = pd.DataFrame(results, columns=["min_return", "window_minutes", "total_return"])
    results_path = os.path.join(DATA_DIR, "morning_filter_results.csv")
    results_df.to_csv(results_path, index=False)
    print(f"✅ Сохранён: {results_path}")

    # График
    plt.figure(figsize=(12, 6))
    if len(base_cumret) > 0:
        plt.plot(base_cumret.index, base_cumret, label="Без фильтра", alpha=0.7)
    if best_params is not None and best_series is not None and len(best_series) > 0:
        best_r, best_w = best_params
        plt.plot(best_series.index, best_series, label=f"Фильтр: +{best_r*100:.2f}% за {best_w} мин", linewidth=2)
        print(f"\n🏆 Лучший фильтр: +{best_r*100:.2f}% за {best_w} мин")
        print(f"   Доходность: {best_return:.2%}")
        print(f"   Базовая:    {base_return:.2%}")
    else:
        plt.text(0.5, 0.5, "Оптимизация не удалась", ha="center", va="center", fontsize=14)
        print("❌ Не удалось построить стратегию")

    plt.title("Оптимизация утреннего фильтра")
    plt.xlabel("Дата")
    plt.ylabel("Накопленная доходность")
    plt.legend()
    plt.grid(alpha=0.3)
    plt.tight_layout()

    plot_path = os.path.join(DATA_DIR, "morning_filter_optimization.png")
    plt.savefig(plot_path)
    plt.close()
    print(f"✅ Сохранён: {plot_path}")


if __name__ == "__main__":
    main()
