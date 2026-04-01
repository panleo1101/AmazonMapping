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

RankedItems AS (
    SELECT 
        it.id, 
        fi.title, 
        it.product_title, 
        ROW_NUMBER() OVER (PARTITION BY it.id ORDER BY fi.title) AS ROW_NUM
    FROM th.TH_application.fits fi
    INNER JOIN th.TH_application.items it
        ON fi.item_id = it.id
    INNER JOIN th.TH_application.product_descriptions pd
        ON pd.id = it.product_description_id
    INNER JOIN th.TH_application.categories cat
        ON cat.id = pd.category_id
    WHERE cat.name IN ('Brake Kits') AND it.discontinued = 0
),

ACCESSORY AS (
	SELECT 
		id,
		CASE 
			WHEN product_title LIKE '% with %' 
			THEN REPLACE(
                SUBSTRING(product_title, CHARINDEX(' with ', product_title) + 1, LEN(product_title)), 
                'Performance ', 
                ''
             )
			ELSE NULL 
		END AS with_description,
		ROW_NUM
	FROM RankedItems
	WHERE ROW_NUM = 1
),


BaseResult AS (
    SELECT
        ym.item_id, 
        ym.MidYear, 
        ym.model, 
        p.position, 
        pd.item_short_description AS Partname,
		ac.with_description,
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
        AND cat.name = 'Brake Kits'
    LEFT JOIN ComponentQty cq
        ON ym.item_id = cq.KitSKU
	LEFT JOIN ACCESSORY ac
		ON ym.item_id = ac.id
    WHERE it.discontinued = 0 
)

SELECT *
FROM BaseResult
WHERE RowNum <=6
"""

print("正在從公司資料庫下載資料...")
df = pd.read_sql(query, sql_server_conn)

# ================= 2. 資料清理與 DataFrame 分流 =================

df['MidYear'] = df['MidYear'].astype(str).str.replace(r'\.0$', '', regex=True)
df['model'] = df['model'].astype(str).str.strip()
df['Partname'] = df['Partname'].astype(str).str.strip()

df['with_description'] = df['with_description'].astype(str).str.strip()
df['position'] = df['position'].astype(str).str.strip()

df.loc[df['with_description'].isin(['None', 'nan', 'NaN', 'NULL']), 'with_description'] = '' 
df.loc[df['position'].isin(['None', 'nan', 'NaN', 'NULL']), 'position'] = '' 

# ----------------- 產生第一個 DF: df_set -----------------
df_copy = df.copy()

df_copy['title'] = (df_copy['MidYear'] + ' ' + 
                    df_copy['model'] + ' ' + 
                    df_copy['position'] + ' ' + 
                    df_copy['Partname'] + ' ' + 
                    df_copy['with_description'])

df_copy['title'] = df_copy['title'].str.replace(r'\s+', ' ', regex=True).str.strip()

# 輸出要求的欄位
df_title = df_copy[['item_id', 'title']]


#================= 3. 寫入本地端 SQLite 資料庫 =================
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

data_to_insert = df_title.to_records(index=False).tolist()

cursor_sqlite.executemany('''
    INSERT OR REPLACE INTO item_title_list (item_id, title)
    VALUES (?, ?)
''', data_to_insert)

sqlite_conn.commit()

cursor_sqlite.close()
sqlite_conn.close()
sql_server_conn.close()

print(f"資料寫入完成！共處理了 {len(data_to_insert)} 筆不重複的資料。")
