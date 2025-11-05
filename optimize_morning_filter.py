import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

DATA_DIR = "data"
ASSETS = ["GOLD", "EQMX", "OBLG"]
RISK_FREE = "LQDT"

def load_data():
    # Загрузка сигналов
    signals_path = os.path.join(DATA_DIR, "signals.csv")
    signals = pd.read_csv(signals_path, parse_dates=["date"])
    signals = signals.set_index("date")["signal"]
    print(f"✅ Загружено {len(signals)} сигналов")

    # Загрузка D1-данных
    d1 = {}
    for asset in ASSETS + [RISK_FREE]:
        path = os.path.join(DATA_DIR, f"{asset}.csv")
        df = pd.read_csv(path, parse_dates=["TRADEDATE"])
        df = df.set_index("TRADEDATE")["CLOSE"].rename(asset)
        d1[asset] = df
    d1_full = pd.concat(d1.values(), axis=1).sort_index()
    print(f"📅 D1 данные: {d1_full.index.min()} — {d1_full.index.max()}")

    # Загрузка M1-данных
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

    # Все торговые дни из D1 (должны быть непрерывными)
    trading_days = d1_full.index.tolist()
    if len(trading_days) < 2:
        return pd.Series([1.0], index=[trading_days[0]] if trading_days else [pd.Timestamp("2023-01-01")])

    for i in range(len(trading_days) - 1):
        date = trading_days[i]        # день D — дата сигнала
        next_date = trading_days[i + 1]  # день D+1 — вход/выход

        # Есть ли сигнал на день D?
        if date in signals.index:
            signal_asset = signals.loc[date]
        else:
            # Нет сигнала → считаем, что остаёмся в LQDT
            signal_asset = RISK_FREE

        if signal_asset == RISK_FREE:
            # Просто держим LQDT
            ret = d1_full.loc[next_date, RISK_FREE] / d1_full.loc[date, RISK_FREE] - 1 - fee
            portfolio *= (1 + ret)
            portfolio_values.append((next_date, portfolio))
            continue

        # Проверяем M1 на next_date (утро D+1)
        m1_df = m1[signal_asset]
        m1_day = m1_df[m1_df.index.date == next_date.date()]

        if len(m1_day) < window_minutes:
            # Нет достаточных M1-данных → укрытие в LQDT
            ret = d1_full.loc[next_date, RISK_FREE] / d1_full.loc[date, RISK_FREE] - 1
            portfolio *= (1 + ret)
            portfolio_values.append((next_date, portfolio))
            continue

        # Цена на открытии и через window_minutes минут
        open_price = m1_day.iloc[0]["open"]
        close_at_window = m1_day.iloc[window_minutes - 1]["close"]
        gain = close_at_window / open_price - 1

        if gain >= min_return:
            # Входим и выходим по закрытию D+1
            entry_price = close_at_window
            exit_price = d1_full.loc[next_date, signal_asset]
            ret = exit_price / entry_price - 1 - 2 * fee  # комиссия на вход и выход
            portfolio *= (1 + ret)
        else:
            # Не входим → LQDT
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

    # Параметры перебора
    min_returns = np.arange(0.0, 0.016, 0.001)  # 0.0% → 1.5%
    window_sizes = [5, 10, 15, 20, 25, 30]       # минуты

    results = []
    best_return = -np.inf
    best_params = None
    best_series = None

    print(f"\n⚙️ Тестирование {len(min_returns) * len(window_sizes)} комбинаций...")
    for r in min_returns:
        for w in window_sizes:
            try:
                cumret = simulate_strategy(signals, d1_full, m1, r, w)
                total_ret = cumret.iloc[-1] - 1
                results.append((r, w, total_ret))
                if total_ret > best_return:
                    best_return = total_ret
                    best_params = (r, w)
                    best_series = cumret
            except Exception as e:
                print(f"⚠️ Ошибка для r={r}, w={w}: {e}")
                continue

    if best_params is None:
        print("❌ Не удалось найти ни одной валидной сделки. Проверьте совпадение дат в данных.")
        return

    # Базовая стратегия (всегда исполняем сигнал без фильтра)
    base_cumret = simulate_strategy(signals, d1_full, m1, min_return=-1.0, window_minutes=1)
    base_return = base_cumret.iloc[-1] - 1

    best_r, best_w = best_params
    print(f"\n🏆 Лучший утренний фильтр:")
    print(f"   Мин. рост: {best_r * 100:.2f}% за {best_w} минут")
    print(f"   Доходность: {best_return:.2%}")
    print(f"   Базовая:    {base_return:.2%}")
    print(f"   Дельта:     {best_return - base_return:+.2%}")

    # График
    plt.figure(figsize=(12, 6))
    plt.plot(base_cumret.index, base_cumret, label="Без фильтра", alpha=0.7, linewidth=1.5)
    plt.plot(best_series.index, best_series, label=f"Фильтр: +{best_r * 100:.2f}% за {best_w} мин", linewidth=2.5)
    plt.title("Оптимизация утреннего фильтра (60 дней)")
    plt.xlabel("Дата")
    plt.ylabel("Накопленная доходность")
    plt.legend()
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(DATA_DIR, "morning_filter_optimization.png"))
    plt.show()

    # Сохранение результатов
    results_df = pd.DataFrame(results, columns=["min_return", "window_minutes", "total_return"])
    results_df.to_csv(os.path.join(DATA_DIR, "morning_filter_results.csv"), index=False)
    print(f"\n✅ Результаты сохранены в:")
    print(f"   data/morning_filter_results.csv")
    print(f"   data/morning_filter_optimization.png")


if __name__ == "__main__":
    main()
