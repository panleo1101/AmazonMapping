import pandas as pd
import numpy as np
import pyodbc
import sqlite3
import warnings
from tabulate import tabulate

warnings.filterwarnings('ignore', category=UserWarning)

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
;WITH yearmodel AS (
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
        AND cat.name = 'Tie Rods & Adjusting Sleeves'
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
    ComponentCount
FROM BaseResult
WHERE RowNum <= 6;

"""

print("正在從公司資料庫下載資料...")
df = pd.read_sql(query, sql_server_conn)

# ================= 2. 資料清理與 DataFrame 分流 =================

# 1. 基礎清理：確保要合併的欄位為字串，並處理可能的浮點數小數點
# 將 MidYear 轉為字串，並移除 '.0'
df['MidYear'] = df['MidYear'].astype(str).str.replace(r'\.0$', '', regex=True)
df['model'] = df['model'].astype(str).str.strip()
df['Partname'] = df['Partname'].astype(str).str.strip()

# 將 Pandas 裡的空值轉換，方便後續字串拼接時不會出現 'nan' 
df['position'] = df['position'].fillna('')
# 如果原本就是字串的 'None' 或 'nan' 也清空，確保邏輯乾淨
df.loc[df['position'].isin(['None', 'nan', 'NaN']), 'position'] = '' 

# ----------------- 產生第一個 DF: df_set -----------------
# 條件：ComponentCount IS NOT NULL
mask_set = df['ComponentCount'].notna()
df_set = df[mask_set].copy()

# 把 ComponentCount 轉成整數再轉成字串 (避免 2 變成 2.0)
df_set['Comp_str'] = df_set['ComponentCount'].astype(int).astype(str)

# 依據 ComponentCount 是否等於 2 給予不同的 title 組合
# 使用 numpy 的 where 條件式來快速分流
df_set['title'] = np.where(
    df_set['ComponentCount'] == 2,
    # ComponentCount == 2
    df_set['MidYear'] + ' ' + df_set['model'] + ' ' + df_set['Partname'],
    # ComponentCount != 2
    df_set['MidYear'] + ' ' + df_set['model'] + ' ' + df_set['Partname'] + ' ' + df_set['Comp_str'] + ' piece'
)

# 輸出要求的欄位
df_set = df_set[['item_id', 'title']]


# ----------------- 產生第二個 DF: df_single -----------------
# 條件：ComponentCount IS NULL AND position 存在實質內容
mask_single = df['ComponentCount'].isna() & (df['position'] != '')
df_single = df[mask_single].copy()

# 組合 title
df_single['title'] = df_single['MidYear'] + ' ' + df_single['model'] + ' ' + df_single['position'] + ' ' + df_single['Partname']

# 去除因為拼接可能產生的多餘空白 (例如剛好某個欄位是空的)
df_single['title'] = df_single['title'].str.replace('  ', ' ').str.strip()

# 輸出要求的欄位
df_single = df_single[['item_id', 'title']]

merged_df = pd.concat([df_single, df_set], ignore_index=True)
# ================= 3. 寫入本地端 SQLite 資料庫 =================
print("正在寫入本地端 SQLite 資料庫...")
sqlite_conn = sqlite3.connect('test.db')
cursor_sqlite = sqlite_conn.cursor()

cursor_sqlite.execute('''
    CREATE TABLE IF NOT EXISTS item_title_list (
        item_id TEXT,
        title TEXT,
        PRIMARY KEY (item_id, title)
    )
''')
sqlite_conn.commit()

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
