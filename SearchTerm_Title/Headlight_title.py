import pandas as pd
import numpy as np
import pyodbc
import sqlite3
import warnings

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
        SUM(
            CASE 
                WHEN ComponentSKU LIKE '1ALHP%' 
                    OR ComponentSKU LIKE '1ALFP%' 
                    OR ComponentSKU LIKE '1ALTP%' 
                THEN ComponentQty + 1
             ELSE ComponentQty 
            END
        ) AS ComponentCount
    FROM erp.item_kit_master
    GROUP BY KitSKU
),

BaseResult AS (
    SELECT
        ym.item_id, 
        ym.MidYear, 
        ym.model, 
        p.position, 
        cat.short_name AS Partname,
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
        AND cat.name = 'Headlight Assemblies'
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
Kit = df[df['ComponentCount'].notna()].copy()
Left_Right = df[df['position'].notna()].copy()

# 處理 Kit 表 (將生成的欄位直接命名為 title)
kit_mid_year = Kit['MidYear'].astype(str).str.replace('.0', '', regex=False)
qty_suffix = Kit['ComponentCount'].apply(
    lambda x: "pair" if x == 2 else f"set {int(x) if float(x).is_integer() else x} piece"
)
Kit['title'] = kit_mid_year + " " + Kit['model'].astype(str) + " " + Kit['Partname'].astype(str) + " " + qty_suffix
Kit_search_term = Kit[['item_id', 'title']]

# 處理 Left_Right 表 (將生成的欄位直接命名為 title)
lr_mid_year = Left_Right['MidYear'].astype(str).str.replace('.0', '', regex=False)
Left_Right['title'] = lr_mid_year + " " + Left_Right['model'].astype(str) + " " + Left_Right['position'].astype(str) + " " + Left_Right['Partname'].astype(str)
Left_Right_search_term = Left_Right[['item_id', 'title']]

# ----------------- 合併兩個表 -----------------
print("正在合併與處理資料...")
merged_df = pd.concat([Kit_search_term, Left_Right_search_term], ignore_index=True)

# 由於資料庫設定了 Primary Key，我們先在 pandas 裡把重複的組合刪掉，以免寫入時產生不必要的忽略操作
merged_df = merged_df.drop_duplicates(subset=['item_id', 'title'])


# ================= 3. 寫入本地端 SQLite 資料庫 =================
print("正在寫入本地端 SQLite 資料庫...")
sqlite_conn = sqlite3.connect('test.db')   #修改成自己的SQLite
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

data_to_insert = merged_df.to_records(index=False).tolist()

.cursor_sqliteexecutemany('''
    INSERT OR IGNORE INTO item_title_list (item_id, title)
    VALUES (?, ?)
''', data_to_insert)

sqlite_conn.commit()

cursor_sqlite.close()
sqlite_conn.close()
sql_server_conn.close()

print(f"資料寫入完成！共處理了 {len(data_to_insert)} 筆不重複的資料。")
