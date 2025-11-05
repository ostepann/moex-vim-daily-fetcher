def simulate_strategy(signals, d1_full, m1, min_return, window_minutes, fee=0.0004):
    portfolio = 1.0
    portfolio_values = []

    trading_days = d1_full.index.tolist()
    if len(trading_days) < 2:
        return pd.Series([1.0], index=[trading_days[0]] if trading_days else [pd.Timestamp("2023-01-01")])

    for i in range(len(trading_days) - 1):
        date = trading_days[i]
        next_date = trading_days[i + 1]

        signal_asset = signals.loc[date] if date in signals.index else RISK_FREE

        if signal_asset == RISK_FREE:
            ret = d1_full.loc[next_date, RISK_FREE] / d1_full.loc[date, RISK_FREE] - 1 - fee
            portfolio *= (1 + ret)
            portfolio_values.append((next_date, portfolio))
            continue

        # --- M1 FILTER ---
        try:
            target_date = pd.Timestamp(next_date).date()
        except:
            # Защита
            ret = d1_full.loc[next_date, RISK_FREE] / d1_full.loc[date, RISK_FREE] - 1
            portfolio *= (1 + ret)
            portfolio_values.append((next_date, portfolio))
            continue

        m1_df = m1[signal_asset]
        if m1_df.index.tz is not None:
            m1_df = m1_df.tz_localize(None)

        m1_day = m1_df[m1_df.index.date == target_date]

        if len(m1_day) < window_minutes:
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
        return pd.Series([1.0], index=[trading_days[0]])
    dates, values = zip(*portfolio_values)
    return pd.Series(values, index=dates)
