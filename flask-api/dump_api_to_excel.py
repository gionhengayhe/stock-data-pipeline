import requests
import pandas as pd
import openpyxl

# Base URL của Flask API
BASE_URL = 'http://localhost:5000'

# Danh sách các endpoint cần truy xuất
endpoints = [
    'dim_time',
    'dim_news',
    'dim_topics',
    'dim_companies',
    'fact_news_companies',
    'fact_news_topics',
    'fact_candles'
]

# Tên file Excel xuất ra
excel_filename = 'exported_data.xlsx'

# Dictionary lưu trữ DataFrame cho mỗi sheet
sheet_data = {}

# Lặp qua từng endpoint và gọi API
for endpoint in endpoints:
    print(f"Fetching data from /{endpoint} ...")
    try:
        response = requests.get(f"{BASE_URL}/{endpoint}")
        response.raise_for_status()
        data = response.json()

        # Convert to DataFrame
        df = pd.DataFrame(data)

        # Lưu vào dict
        sheet_data[endpoint] = df
    except Exception as e:
        print(f"Lỗi khi fetch {endpoint}: {e}")

# Ghi tất cả các sheet vào file Excel
with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
    for sheet_name, df in sheet_data.items():
        df.to_excel(writer, sheet_name=sheet_name, index=False)

print(f"\n✅ Dữ liệu đã được lưu vào file: {excel_filename}")
