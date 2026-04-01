import pandas as pd
import numpy as np
import pyodbc
import sqlite3
import warnings
from tabulate import tabulate

warnings.filterwarnings('ignore', category=UserWarning)

# ================= 1. Connecting DB =================

server = 'prodaag'
database = 'th'
username = 'read'
password = 'read_only'

sql_server_conn = pyodbc.connect(
    f'DRIVER={{ODBC Driver 17 for SQL Server}};'
    f'SERVER={server};'
    f'DATABASE={database};' 
    f'UID={username};'
    f'PWD={password}'
)

query = """
;;WITH yearmodel AS (
    SELECT
        a.item_id, 
        (MIN(b.year_value) + MAX(b.year_value)) / 2 AS 'MidYear', 
        vmo.name AS model
    FROM th.TH_application.fits a
    INNER JOIN th.TH_application.fits_years b
        ON a.id = b.fit_id
    INNER JOIN th.TH_application.vehicle_models vmo
        ON vmo.id = a.vehicle_model_id
    INNER JOIN th.TH_application.vehicle_makes vma
        ON vma.id = vmo.vehicle_make_id
    GROUP BY 
        a.item_id, 
        vmo.name
),

position AS (
    SELECT DISTINCT 
        item_id, 
        position, 
        position_id
    FROM TH_application.fits_aces as abc
),

ComponentQty AS (
    SELECT 
        KitSKU,
        SUM(ComponentQty) AS ComponentCount
    FROM erp.item_kit_master
    GROUP BY KitSKU
),

BaseResult AS (
    SELECT
        ym.item_id, 
        ym.MidYear, 
        ym.model, 
        p.position, 
        pd.item_short_description AS Partname,
        cq.ComponentCount,
        p.position_id,
        cq.KitSKU,
		ROW_NUMBER() OVER(PARTITION BY ym.item_id ORDER BY ym.model) as RowNum
    FROM yearmodel ym
    INNER JOIN position p
        ON p.item_id = ym.item_id
    INNER JOIN th.TH_application.items it
        ON it.id = ym.item_id
    INNER JOIN th.TH_application.product_descriptions pd
        ON pd.id = it.product_description_id
    INNER JOIN th.TH_application.categories cat
        ON cat.id = pd.category_id
        AND cat.name = 'Splash Shields & Fender Liners'
    LEFT JOIN ComponentQty cq
        ON ym.item_id = cq.KitSKU
    WHERE it.discontinued = 0 
)

SELECT 
    item_id, 
    MidYear, 
    model, 
    position, 
    Partname,
    ComponentCount,
    position_id,
    KitSKU
FROM BaseResult
WHERE RowNum <= 6;
"""

print("正在從公司資料庫下載資料...")
df = pd.read_sql(query, sql_server_conn)

# ================= 2. Pandas Data Cleaning =================

# 將 MidYear 轉為字串，並移除可能因為 Pandas 讀取為 float 而產生的 ".0"
df['MidYear'] = df['MidYear'].astype(str).str.replace(r'\.0$', '', regex=True)
df['model'] = df['model'].astype(str)
df['position'] = df['position'].astype(str)
df['Partname'] = df['Partname'].astype(str)

# ----------------- 產生第一個 DF: df_set -----------------
# 條件：ComponentCount IS NOT NULL
mask_set = df['ComponentCount'].notna()
df_set = df[mask_set].copy()

# 判斷 Partname 是否包含 "set" (不區分大小寫)
has_set = df_set['Partname'].str.contains('set', case=False, na=False)

# 組合基本的 title (MidYear + model + Partname)
df_set['title'] = df_set['MidYear'] + ' ' + df_set['model'] + ' ' + df_set['Partname']

# 如果 Partname 裡面沒有 "set"，則在字尾加上 " Set"
df_set.loc[~has_set, 'title'] = df_set['title'] + ' Set'

# 只保留 item_id 和 title
df_set = df_set[['item_id', 'title']]


# ----------------- 產生第二個 DF: df_single -----------------
mask_single = df['ComponentCount'].isna() & df['position'].notna()
df_single = df[mask_single].copy()

df_single['title'] = df_single['MidYear'] + ' ' + df_single['model'] + ' ' + df_single['position'] + ' ' + df_single['Partname']

df_single = df_single[['item_id', 'title']]

print(tabulate(df_set.head(50), headers='keys', tablefmt='psql'))
print(tabulate(df_single.head(500), headers='keys', tablefmt='psql'))

merged_df = pd.concat([df_single, df_set], ignore_index=True)

# 由於資料庫設定了 Primary Key，先在 pandas 裡把重複的組合刪掉
merged_df = merged_df.drop_duplicates(subset=['item_id', 'title'])


# ================= 3. 寫入本地端 SQLite 資料庫 =================
print("正在寫入本地端 SQLite 資料庫...")
sqlite_conn = sqlite3.connect('test.db')
cursor_sqlite = sqlite_conn.cursor()

# 建立 Table (以 item_id 與 title 作為主鍵)
cursor_sqlite.execute('''
    CREATE TABLE IF NOT EXISTS item_title_list (
        item_id TEXT,
        title TEXT,
        PRIMARY KEY (item_id, title)
    )
''')
sqlite_conn.commit()

# 將 Pandas DataFrame 轉為 tuples 列表，以供 sqlite 批次寫入
data_to_insert = merged_df.to_records(index=False).tolist()

cursor_sqlite.executemany('''
    INSERT OR REPLACE INTO item_title_list (item_id, title)
    VALUES (?, ?)
''', data_to_insert)

sqlite_conn.commit()

cursor_sqlite.close()
sqlite_conn.close()
sql_server_conn.close()

print(f"資料寫入完成！共處理了 {len(data_to_insert)} 筆不重複的資料。")
