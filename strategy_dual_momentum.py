def get_and_send_signal():
    df = load_and_prepare_data()
    df = df.set_index('Date').sort_index()

    if len(df) < LOOKBACK + 1:
        msg = f"❌ Недостаточно данных для расчёта Dual Momentum (нужно ≥{LOOKBACK + 1} дней)"
        print(msg)
        send_telegram_message(msg)
        return

    # ✅ OBLG теперь в рисковых активах
    risk_assets = ['GOLD', 'EQMX', 'OBLG']
    risk_free = 'LQDT'
    last_date = df.index[-1]

    # Получаем текущий RVI
    current_rvi = df['Close_RVI'].iloc[-1]

    # Рассчитываем моментум для всех активов
    mom = {}
    for asset in risk_assets + [risk_free]:
        price_today = df[f'Close_{asset}'].iloc[-1]
        price_past = df[f'Close_{asset}'].iloc[-(LOOKBACK + 1)]
        mom[asset] = price_today / price_past - 1

    # Применяем RVI-фильтр
    if current_rvi > RVI_THRESHOLD:
        selected = risk_free
        rvi_note = f"⚠️ RVI = {current_rvi:.2f} > {RVI_THRESHOLD} → вход в рисковые активы запрещён"
    else:
        # Относительный моментум: выбираем лучший из risk_assets
        best_risk = max(risk_assets, key=lambda x: mom[x])
        # Абсолютный моментум: сравниваем с LQDT
        if mom[best_risk] > mom[risk_free]:
            selected = best_risk
        else:
            selected = risk_free
        rvi_note = f"✅ RVI = {current_rvi:.2f} ≤ {RVI_THRESHOLD} → фильтр пройден"

    # Формируем сообщение
    msg_lines = [
        f"📊 *Dual Momentum Signal*",
        f"Дата данных: {last_date.strftime('%Y-%m-%d')}",
        f"Рекомендация: вложить 100% в *{selected}*",
        rvi_note,
        "",
        f"*Моментум ({LOOKBACK} дн.):*"
    ]

    for a in risk_assets + [risk_free]:
        if a == selected:
            sign = "🟢"
        elif a in risk_assets and mom[a] == max(mom[ra] for ra in risk_assets):
            # Подсвечиваем лучший рисковый актив, даже если он не выбран (из-за LQDT)
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
