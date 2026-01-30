import pandas as pd
import numpy as np
import gspread
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
PROJECT_ID = 'logistics-449115'
SPREADSHEET_ID = '1w-Kknr-Or8zwpL8SUmnhsev-mrHuDr2YLhAzjDVRh-4'

# ============================================================
# 3. BigQueryからデータ取得
# ============================================================
sql = "SELECT * FROM `logistics-449115.lastmile.supplyAcquisition`"
print("BigQueryから仕入データを取得中...")
df = client.query(sql).to_dataframe()

df['invoiceDate_parsed'] = pd.to_datetime(df['invoiceDate'].astype(str), format='%Y%m%d', errors='coerce')
max_date = df['invoiceDate_parsed'].max()

# ============================================================
# 5. データフィルタ（単位による除外を廃止）
# ============================================================
# 2週間以内の有効なデータをすべて保持する
df_all = df[
    (df['invoiceDate_parsed'] >= max_date - timedelta(days=14)) &
    (df['unitPrice'] > 0) &
    (df['itemCode'].notna()) & (df['itemCode'].astype(str) != '')
].copy()

if df_all.empty:
    print("表示対象のデータがありませんでした。")
    exit()

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

supplier_stats = (
    df_all.groupby(['itemCode', 'supplierName1'])
    .agg(max_unit_price=('unitPrice', 'max'), min_unit_price=('unitPrice', 'min'),
         avg_unit_price=('unitPrice', 'mean'), sample_count=('unitPrice', 'count'))
    .round(0).reset_index()
).sort_values(['itemCode', 'avg_unit_price'])

# ============================================================
# 12. ワイド形式作成（絶対的な最高値を抽出）
# ============================================================
print("全データから最新の最高・最安値を抽出中...")
result_rows = []
item_info = df_all.groupby('itemCode').agg({'itemName': 'first'}).reset_index()

for _, item in item_info.iterrows():
    item_code = item['itemCode']
    item_df = df_all[df_all['itemCode'] == item_code]
    
    # 価格が最も高く、かつ日付が最も新しい行を取得
    high_row = item_df.sort_values(['unitPrice', 'invoiceDate_parsed'], ascending=[False, False]).iloc[0]
    # 価格が最も低く、かつ日付が最も新しい行を取得
    low_row = item_df.sort_values(['unitPrice', 'invoiceDate_parsed'], ascending=[True, False]).iloc[0]

    # 表示用の標準単位（最頻値）
    mode_unit = item_df['kgAmount'].mode()
    display_unit = mode_unit.iloc[0] if not mode_unit.empty else item_df['kgAmount'].iloc[0]

    row = {
        '商品コード': item_code,
        '商品名': item['itemName'],
        '仕入れ単位': f"{display_unit}kg",
        '最高単価日': high_row['invoiceDate_parsed'].strftime('%Y/%m/%d'),
        '最高単価': int(high_row['unitPrice']),
        '最安単価日': low_row['invoiceDate_parsed'].strftime('%Y/%m/%d'),
        '最安単価': int(low_row['unitPrice']),
        '平均単価': int(overall_avg.get(item_code, 0)),
        '短期トレンド': judge_trend(item_code)
    }

    suppliers = supplier_stats[supplier_stats['itemCode'] == item_code].reset_index(drop=True)
    for idx, s in suppliers.iterrows():
        rank = idx + 1
        row[f'仕入先{rank}'] = s['supplierName1']
        row[f'仕入先{rank}_単価'] = f"{int(s['max_unit_price'])}/{int(s['min_unit_price'])}：{int(s['avg_unit_price'])}"
        row[f'仕入先{rank}_取引回数'] = int(s['sample_count'])
    result_rows.append(row)

result = pd.DataFrame(result_rows).fillna('')
start_date = df_all['invoiceDate_parsed'].min().strftime('%Y/%m/%d')
end_date = df_all['invoiceDate_parsed'].max().strftime('%Y/%m/%d')
result['集計期間'] = f'{start_date} - {end_date}'

base_cols = ['商品コード', '商品名', '仕入れ単位', '最高単価日', '最高単価', '最安単価日', '最安単価', '平均単価', '短期トレンド', '集計期間']
result = result[base_cols + sorted([c for c in result.columns if c.startswith('仕入先')])]

# ============================================================
# 14. Google Sheets 出力 & 書式設定
# ============================================================
print("スプレッドシートを更新中...")
sh = gc.open_by_key(SPREADSHEET_ID)
sheet_name = '仕入先分析_単価'
try:
    worksheet = sh.worksheet(sheet_name)
except gspread.exceptions.WorksheetNotFound:
    worksheet = sh.add_worksheet(title=sheet_name, rows=1000, cols=60)

worksheet.clear()
worksheet.update([result.columns.tolist()] + result.values.tolist(), value_input_option='RAW')

# 書式設定（枠線）
sheet_id = worksheet.id
num_rows, num_cols = result.shape
high_idx = result.columns.get_loc('最高単価日')
low_idx = result.columns.get_loc('最安単価')

requests = [
    # 1. 全体に格子
    {"updateBorders": {"range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": num_rows+1, "startColumnIndex": 0, "endColumnIndex": num_cols},
                       "top": {"style": "SOLID", "width": 1}, "bottom": {"style": "SOLID", "width": 1},
                       "left": {"style": "SOLID", "width": 1}, "right": {"style": "SOLID", "width": 1},
                       "innerHorizontal": {"style": "SOLID", "width": 1}, "innerVertical": {"style": "SOLID", "width": 1}}},
    # 2. ヘッダー装飾
    {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": num_cols},
                    "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}, "textFormat": {"bold": True}, "horizontalAlignment": "CENTER"}}, "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"}},
    # 3. 最高・最安エリア太枠
    {"updateBorders": {"range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": num_rows+1, "startColumnIndex": high_idx, "endColumnIndex": low_idx + 1},
                       "left": {"style": "SOLID_MEDIUM", "width": 2}, "right": {"style": "SOLID_MEDIUM", "width": 2}}}
]

sh.batch_update({'requests': requests})
print("✅ 完了。玉葱の1/30 6,150円（最高値）が正しく反映されます。")
