import requests
import pandas as pd
import os
from datetime import datetime

os.makedirs("data", exist_ok=True)

url = "https://iss.moex.com/iss/engines/stock/markets/shares/securities/eqmx/candles.json?from=2025-11-01&till=2025-11-01&interval=1"

print(f"📥 Запрашиваю данные: {url}")

try:
    response = requests.get(url, timeout=20)
    response.raise_for_status()
    raw = response.json()
except Exception as e:
    print(f"❌ Ошибка запроса: {e}")
    exit(1)

# Извлекаем данные из вложенного объекта "candles"
if "candles" not in raw:
    print("⚠️ В ответе отсутствует ключ 'candles'")
    exit(1)

candles = raw["candles"]
columns = candles.get("columns")
data_rows = candles.get("data", [])

if not columns or not data_rows:
    print("⚠️ Нет данных или структура ответа неожиданная")
    exit(1)

# Создаём DataFrame
df = pd.DataFrame(data_rows, columns=columns)
df['begin'] = pd.to_datetime(df['begin'])

# Фильтруем 09:59–10:59
df_filtered = df[
    (df['begin'].dt.time >= pd.Timestamp("09:59").time()) &
    (df['begin'].dt.time <= pd.Timestamp("10:59").time())
].copy()

print(f"✅ Получено {len(data_rows)} свечей, после фильтра: {len(df_filtered)}")

# Сохраняем
output_file = "data/EQMX_M1_0959_1059_20251101.CSV"
df_filtered.to_csv(output_file, index=False, date_format='%Y-%m-%d %H:%M:%S')
print(f"💾 Сохранено в: {output_file}")
