def generate_signal(ticker):
    df = load_csv(DAILY_PATHS[ticker])
    current_price = df['close'].iloc[-1]
    current_volume = df['volume'].iloc[-1]

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

    # --- Динамика цены ---
    price_changes = {}
    for days in PRICE_DYNAMICS:
        if len(df) > days:
            past_price = df['close'].iloc[-(days + 1)]
            change_pct = (current_price - past_price) / past_price * 100
            price_changes[days] = change_pct
        else:
            price_changes[days] = None

    # --- Динамика объёма (1, 5, 10 дней) ---
    volume_ratios = {}
    for days in PRICE_DYNAMICS:  # [1, 5, 10]
        if len(df) > days:
            avg_vol = df['volume'].iloc[-(days + 1):-1].mean()  # за предыдущие N дней
            ratio = current_volume / avg_vol if avg_vol > 0 else 1.0
            volume_ratios[days] = ratio
        else:
            volume_ratios[days] = None

    def format_volume_ratios(ratios):
        parts = []
        for days in [1, 5, 10]:
            if ratios[days] is not None:
                parts.append(f"{ratios[days]:.1f}x за {days} дн")
        return ", ".join(parts) if parts else "недостаточно данных"

    volume_desc = format_volume_ratios(volume_ratios)

    # --- Уровни ---
    supports, resistances = find_levels(df)
    nearby_supports = [level for level in supports if abs(current_price - level) / current_price < 0.015]
    nearby_resistances = [level for level in resistances if abs(current_price - level) / current_price < 0.015]

    # --- Сигнал и интерпретация ---
    signal = "HOLD"
    interpretation = ""

    if price_changes[1] is not None and price_changes[5] is not None:
        short_trend = "рост" if price_changes[1] > 0 else "падение"
    else:
        short_trend = "недостаточно данных"

    # Используем объём за 5 дней для фильтра (можно настроить)
    vol_5d = volume_ratios.get(5, 0) or 0

    if nearby_supports and vol_5d > 1.5 and check_confirmation_h1(ticker):
        interpretation = f"Цена у поддержки, объём высокий → возможен отскок ({short_trend})"
        if current_price > current_ema:
            signal = "BUY"
    elif nearby_resistances and vol_5d > 1.5 and check_confirmation_h1(ticker):
        interpretation = f"Цена у сопротивления, объём высокий → возможен разворот ({short_trend})"
        if current_price < current_ema:
            signal = "SELL"
    elif vol_5d > 1.8 and current_price > current_ema and price_changes[5] and price_changes[5] > 0:
        interpretation = "Сильный восходящий тренд + высокий объём → продолжение роста"
        signal = "BUY"
    else:
        interpretation = "Нет чёткого сигнала"

    return {
        "ticker": ticker,
        "price": current_price,
        "price_changes": price_changes,
        "ema_span": ema_span,
        "ema_value": current_ema,
        "ema_trend": ema_trend,
        "volume_desc": volume_desc,
        "supports": sorted(nearby_supports),
        "resistances": sorted(nearby_resistances),
        "signal": signal,
        "interpretation": interpretation,
        "rvi": rvi
    }
