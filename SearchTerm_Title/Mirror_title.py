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
    FROM 
        erp.item_kit_master
    GROUP BY 
        KitSKU
),

MainTable AS (
    SELECT
        ym.item_id, 
        ym.MidYear, 
        ym.model, 
        p.position, 
		CASE
			WHEN cat.name = 'Mirror Glass - Side View' THEN 'Mirror Glass'
			WHEN cat.name = 'Mirror - Side View' THEN 'Mirror'
			WHEN cat.name = 'Mirror Cover' THEN 'Mirror Cap'
		END AS Partname,
		CASE 
			WHEN cq.ComponentCount = 2 THEN 'pair'
			WHEN cq.ComponentCount <> 2 THEN CONCAT(CAST(cq.ComponentCount AS INT), ' piece')
			ELSE NULL
		END AS ComponentCount, 
		p.position_id,
		cq.KitSKU,
		ROW_NUMBER() OVER (PARTITION BY ym.item_id ORDER BY ym.MidYear DESC) AS RN
    FROM yearmodel ym
    INNER JOIN position p
        ON p.item_id = ym.item_id
	INNER JOIN th.TH_application.items it
	ON it.id = ym.item_id
	INNER JOIN th.TH_application.product_descriptions pd
	ON pd.id = it.product_description_id
	INNER JOIN th.TH_application.categories cat
	ON cat.id = pd.category_id
	AND cat.name IN ('Mirror - Side View', 'Mirror Cover', 'Mirror Glass - Side View')
	LEFT JOIN ComponentQty cq
	ON ym.item_id = cq.KitSKU
	WHERE it.discontinued = 0
)

SELECT MidYear, model, position, Partname, ComponentCount
FROM MainTable
WHERE RN <= 6
ORDER BY item_id
"""

print("正在從公司資料庫下載資料...")
df = pd.read_sql(query, sql_server_conn)

position_mapping = {
    'Left': 'Driver Side',
    'Right': 'Passenger Side',
    'Driver Side': 'Left',
    'Passenger Side': 'Right'
}

df_new_rows = df[df['position'].isin(position_mapping.keys())].copy()
df_new_rows['position'] = df_new_rows['position'].map(position_mapping)

df_final = pd.concat([df, df_new_rows], ignore_index=True)


def build_merged_info(row):

    mid_year = str(row['MidYear']).replace('.0', '') if pd.notnull(row['MidYear']) else ""
    
    components = [
        mid_year,
        str(row['model']) if pd.notnull(row['model']) else "",
        str(row['position']) if pd.notnull(row['position']) else "",
        str(row['Partname']) if pd.notnull(row['Partname']) else "",
        str(row['ComponentCount']) if pd.notnull(row['ComponentCount']) else ""
    ]
    
    clean_components = [str(c).strip() for c in components if pd.notnull(c) and str(c).strip() != "" and str(c).lower() != "none"]
    
    return " ".join(clean_components)

df_final['Merged_Info'] = df_final.apply(build_merged_info, axis=1)

print("資料延伸與合併完成！")
print(tabulate(df_final[['Merged_Info']].head(50), 
               headers='keys', tablefmt='psql'))
