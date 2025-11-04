import requests
import pandas as pd
import os
from datetime import datetime

# Создаём папку data
os.makedirs("data", exist_ok=True)

# Фиксированный URL
url = "https://iss.moex.com/iss/engines/stock/markets/shares/securities/eqmx/candles.json?from=2025-11-01&till=2025-11-01&interval=1"

print(f"📥 Запрашиваю данные: {url}")

try:
    response = requests.get(url, timeout=20)
    response.raise_for_status()
    data = response.json()
except Exception as e:
    print(f"❌ Ошибка запроса: {e}")
    exit(1)

# Проверяем структуру
if len(data) < 2 or not data[1]:
    print("⚠️ Ответ пустой или неверная структура")
    exit(1)

columns = data[0]['columns']
rows = data[1]

if not rows:
    print("⚠️ Нет данных в ответе")
    exit(1)

# Преобразуем в DataFrame
df = pd.DataFrame(rows, columns=columns)
df['begin'] = pd.to_datetime(df['begin'])

# Фильтр: 09:59:00 – 10:59:59 включительно
df_filtered = df[
    (df['begin'].dt.time >= pd.Timestamp("09:59").time()) &
    (df['begin'].dt.time <= pd.Timestamp("10:59").time())
].copy()

print(f"✅ Получено {len(rows)} свечей, после фильтра: {len(df_filtered)}")

# Сохраняем
output_file = "data/EQMX_M1_0959_1059_20251101.CSV"
df_filtered.to_csv(output_file, index=False, date_format='%Y-%m-%d %H:%M:%S')

print(f"💾 Сохранено в: {output_file}")
