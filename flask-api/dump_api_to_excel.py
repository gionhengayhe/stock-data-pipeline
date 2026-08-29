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
page_size = 5000

# Lặp qua từng endpoint và gọi API
for endpoint in endpoints:
    print(f"Fetching data from /{endpoint} ...")
    try:
        rows = []
        offset = 0
        while True:
            response = requests.get(
                f"{BASE_URL}/{endpoint}",
                params={"limit": page_size, "offset": offset},
                timeout=30,
            )
            response.raise_for_status()
            page = response.json()
            rows.extend(page)
            if len(page) < page_size:
                break
            offset += page_size

        df = pd.DataFrame(rows)

        # Lưu vào dict
        sheet_data[endpoint] = df
    except Exception as e:
        print(f"Lỗi khi fetch {endpoint}: {e}")

# Ghi tất cả các sheet vào file Excel
with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
    for sheet_name, df in sheet_data.items():
        df.to_excel(writer, sheet_name=sheet_name, index=False)

print(f"\n✅ Dữ liệu đã được lưu vào file: {excel_filename}")
