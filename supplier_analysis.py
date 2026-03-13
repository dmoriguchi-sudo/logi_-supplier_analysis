import pandas as pd
import numpy as np
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2 import service_account
from google.cloud import bigquery
from datetime import datetime, timedelta
import os
import json

# ============================================================
# 1. 認証設定
# ============================================================
env_key = os.getenv("GCP_SA_KEY")
if not env_key:
    raise ValueError("GCP_SA_KEY is not set")

info = json.loads(env_key)
creds = service_account.Credentials.from_service_account_info(info, scopes=[
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/bigquery',
    'https://www.googleapis.com/auth/cloud-platform'
])

gc = gspread.authorize(creds)
client = bigquery.Client(credentials=creds, project='logistics-449115')

# ============================================================
# 2. 基本設定
# ============================================================
SPREADSHEET_ID = '1w-Kknr-Or8zwpL8SUmnhsev-mrHuDr2YLhAzjDVRh-4'

# ============================================================
# 3. BigQueryからデータ取得
# ============================================================
sql = "SELECT * FROM `logistics-449115.lastmile.supplyAcquisition`"
print("BigQueryから仕入データを取得中...")
df = client.query(sql).to_dataframe()

# スキーマに基づき、unitOfQuantity(I列)を使用
unit_col = "unitOfQuantity" 

df['invoiceDate_parsed'] = pd.to_datetime(df['invoiceDate'].astype(str), format='%Y%m%d', errors='coerce')
max_date = df['invoiceDate_parsed'].max()

# 直近14日間のデータ（値引き・請求違い・調整金コード除外）
EXCLUDE_CODES = {'9999997', '9999998', '9999999'}
df_all = df[
    (df['invoiceDate_parsed'] >= max_date - timedelta(days=14)) &
    (df['unitPrice'] > 0) &
    (df['itemCode'].notna()) &
    (~df['itemCode'].astype(str).isin(EXCLUDE_CODES))
].copy()

# ============================================================
# 8-11. 統計・トレンド計算
# ============================================================
overall_avg = df_all.groupby('itemCode')['unitPrice'].mean().round(0)
df_short = df_all[df_all['invoiceDate_parsed'] >= max_date - timedelta(days=7)]
short_avg = df_short.groupby('itemCode')['unitPrice'].mean().round(0)

def judge_trend(item_code):
    overall = overall_avg.get(item_code)
    short = short_avg.get(item_code)
    if overall is None or short is None: return 'FLAT'
    return 'UP' if short > overall else 'DOWN' if short < overall else 'FLAT'

# ============================================================
# 12. ワイド形式作成（商品コード順に並べ替えを追加）
# ============================================================
print("データを集計中...")
result_rows = []
item_codes = df_all['itemCode'].unique()

for code in item_codes:
    item_df = df_all[df_all['itemCode'] == code]
    if item_df.empty: continue
    
    # 最高単価を基準に特定
    high_row = item_df.sort_values(['unitPrice', 'invoiceDate_parsed'], ascending=[False, False]).iloc[0]
    high_price = high_row['unitPrice']
    high_date = high_row['invoiceDate_parsed'].strftime('%Y/%m/%d')
    base_unit = high_row[unit_col]
    
    # 同じ単位の中で最低単価を検索
    same_unit_df = item_df[item_df[unit_col] == base_unit]
    if not same_unit_df.empty:
        low_row = same_unit_df.sort_values(['unitPrice', 'invoiceDate_parsed'], ascending=[True, False]).iloc[0]
    else:
        low_row = high_row
    
    low_price = low_row['unitPrice']
    low_date = low_row['invoiceDate_parsed'].strftime('%Y/%m/%d')

    row = {
        '商品コード': code,
        '商品名': item_df['itemName'].iloc[0],
        '仕入れ単位': f"{base_unit}",
        '最高単価日': high_date,
        '最高単価': int(high_price),
        '最安単価日': low_date,
        '最安単価': int(low_price),
        '平均単価': int(overall_avg.get(code, 0)),
        '短期トレンド': judge_trend(code)
    }

    # 仕入先別統計
    suppliers = (
        item_df.groupby('supplierName1')
        .agg(max_p=('unitPrice', 'max'), min_p=('unitPrice', 'min'), avg_p=('unitPrice', 'mean'), cnt=('unitPrice', 'count'))
        .reset_index().sort_values('avg_p')
    )
    
    for idx, s in suppliers.reset_index(drop=True).iterrows():
        rank = idx + 1
        row[f'仕入先{rank}'] = s['supplierName1']
        row[f'仕入先{rank}_単価'] = f"{int(s['max_p'])}/{int(s['min_p'])}：{int(s['avg_p'])}"
        row[f'仕入先{rank}_取引回数'] = int(s['cnt'])
    result_rows.append(row)

# データフレーム作成
result = pd.DataFrame(result_rows)

# ★ ここで商品コード(A列相当)で昇順に並べ替え ★
result = result.sort_values('商品コード').fillna('')

# 列の並びを整理（仕入先を数値順・サフィックス順にソート）
import re
def _supplier_sort_key(col):
    m = re.match(r'仕入先(\d+)(.*)', col)
    if not m:
        return (999, 0)
    return (int(m.group(1)), {'': 0, '_単価': 1, '_取引回数': 2}.get(m.group(2), 99))

base_cols = ['商品コード', '商品名', '仕入れ単位', '最高単価日', '最高単価', '最安単価日', '最安単価', '平均単価', '短期トレンド']
supplier_cols = sorted([c for c in result.columns if c.startswith('仕入先')], key=_supplier_sort_key)
result = result[base_cols + supplier_cols]

# ============================================================
# 14. Google Sheets 出力
# ============================================================
sh = gc.open_by_key(SPREADSHEET_ID)
worksheet = sh.worksheet('仕入先分析_単価')
sheet_id = worksheet.id

# フィルター解除 → クリア → 書き込み（フィルター残存によるズレを防ぐ）
sh.batch_update({'requests': [{"clearBasicFilter": {"sheetId": sheet_id}}]})
worksheet.clear()
set_with_dataframe(worksheet, result, include_index=False, include_column_header=True)
num_rows, num_cols = result.shape
high_idx = result.columns.get_loc('最高単価日')
low_idx = result.columns.get_loc('最安単価')

requests = [
    {"updateBorders": {"range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": num_rows+1, "startColumnIndex": 0, "endColumnIndex": num_cols},
                       "top": {"style": "SOLID", "width": 1}, "bottom": {"style": "SOLID", "width": 1},
                       "left": {"style": "SOLID", "width": 1}, "right": {"style": "SOLID", "width": 1},
                       "innerHorizontal": {"style": "SOLID", "width": 1}, "innerVertical": {"style": "SOLID", "width": 1}}},
    {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": num_cols},
                    "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}, "textFormat": {"bold": True}, "horizontalAlignment": "CENTER"}}, "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"}},
    {"updateBorders": {"range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": num_rows+1, "startColumnIndex": high_idx, "endColumnIndex": low_idx + 1},
                       "left": {"style": "SOLID_MEDIUM", "width": 2}, "right": {"style": "SOLID_MEDIUM", "width": 2}}}
]
sh.batch_update({'requests': requests})
print("✅ 商品コード順に並べ替えて更新を完了しました。")
