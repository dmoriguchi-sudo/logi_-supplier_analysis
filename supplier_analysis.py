import pandas as pd
import numpy as np
import gspread
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
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
if "service_account" in info.get("type", ""):
    creds = service_account.Credentials.from_service_account_info(info, scopes=[
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/bigquery',
        'https://www.googleapis.com/auth/cloud-platform'
    ])
else:
    creds = Credentials.from_authorized_user_info(info, scopes=[
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
# 5. データフィルタ & 仕入れ単位統一
# ============================================================
df_2weeks = df[
    (df['invoiceDate_parsed'] >= max_date - timedelta(days=14)) &
    (df['unitPrice'] > 0) & (df['kgAmount'] > 0) &
    (df['itemCode'].notna()) & (df['itemCode'].astype(str) != '') &
    (df['supplierName1'].notna()) & (df['supplierName1'] != '')
].copy()

if df_2weeks.empty:
    print("表示対象のデータがありませんでした。")
    exit()

standard_unit_map = (
    df_2weeks.groupby('itemCode')['kgAmount']
    .agg(lambda x: x.mode().iloc[0] if not x.mode().empty else x.iloc[0])
    .to_dict()
)
df_2weeks['standard_unit'] = df_2weeks['itemCode'].map(standard_unit_map)
df_2weeks = df_2weeks[df_2weeks['kgAmount'] == df_2weeks['standard_unit']].copy()

# ============================================================
# 8-11. 単価統計・トレンド計算
# ============================================================
overall_avg = df_2weeks.groupby('itemCode')['unitPrice'].mean().round(0)
df_short = df_2weeks[df_2weeks['invoiceDate_parsed'] >= max_date - timedelta(days=7)]
short_avg = df_short.groupby('itemCode')['unitPrice'].mean().round(0)

def judge_trend(item_code):
    overall = overall_avg.get(item_code)
    short = short_avg.get(item_code)
    if overall is None or short is None: return 'FLAT'
    if short > overall: return 'UP'
    elif short < overall: return 'DOWN'
    else: return 'FLAT'

supplier_stats = (
    df_2weeks.groupby(['itemCode', 'supplierName1'])
    .agg(max_unit_price=('unitPrice', 'max'), min_unit_price=('unitPrice', 'min'),
         avg_unit_price=('unitPrice', 'mean'), sample_count=('unitPrice', 'count'))
    .round(0).reset_index()
)
supplier_stats = supplier_stats.sort_values(['itemCode', 'avg_unit_price'])

# ============================================================
# 12. ワイド形式作成（日付列を独立して追加）
# ============================================================
print("日付列を分離したワイド形式でデータを作成中...")
result_rows = []
item_info = df_2weeks.groupby('itemCode').agg({'itemName': 'first', 'standard_unit': 'first'}).reset_index()

for _, item in item_info.iterrows():
    item_code = item['itemCode']
    item_df = df_2weeks[df_2weeks['itemCode'] == item_code]
    
    if item_df.empty: continue

    # 最高・最安とその発生日
    highest_price = item_df['unitPrice'].max()
    highest_date = item_df[item_df['unitPrice'] == highest_price]['invoiceDate_parsed'].max().strftime('%Y/%m/%d')
    lowest_price = item_df['unitPrice'].min()
    lowest_date = item_df[item_df['unitPrice'] == lowest_price]['invoiceDate_parsed'].max().strftime('%Y/%m/%d')

    row = {
        '商品コード': item_code,
        '商品名': item['itemName'],
        '仕入れ単位': f"{item['standard_unit']}kg",
        '最高単価': int(highest_price),
        '最高単価日': highest_date,
        '最安単価': int(lowest_price),
        '最安単価日': lowest_date,
        '平均単価': int(overall_avg.get(item_code, 0)),
        '短期トレンド': judge_trend(item_code)
    }

    # 仕入先列の追加
    suppliers = supplier_stats[supplier_stats['itemCode'] == item_code].reset_index(drop=True)
    for idx, s in suppliers.iterrows():
        rank = idx + 1
        row[f'仕入先{rank}'] = s['supplierName1']
        row[f'仕入先{rank}_単価'] = f"{int(s['max_unit_price'])}/{int(s['min_unit_price'])}：{int(s['avg_unit_price'])}"
        row[f'仕入先{rank}_取引回数'] = int(s['sample_count'])
    result_rows.append(row)

result = pd.DataFrame(result_rows).fillna('')

# 並び順の整理
start_date = df_2weeks['invoiceDate_parsed'].min().strftime('%Y/%m/%d')
end_date = df_2weeks['invoiceDate_parsed'].max().strftime('%Y/%m/%d')
result['集計期間'] = f'{start_date} - {end_date}'

base_cols = ['商品コード', '商品名', '仕入れ単位', '最高単価', '最高単価日', '最安単価', '最安単価日', '平均単価', '短期トレンド', '集計期間']
supplier_cols = sorted([c for c in result.columns if c.startswith('仕入先')])
result = result[base_cols + supplier_cols]

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
total_rows = num_rows + 1
high_idx = result.columns.get_loc('最高単価')
low_date_idx = result.columns.get_loc('最安単価日')

requests = [
    # 1. 全体に格子
    {"updateBorders": {"range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": total_rows, "startColumnIndex": 0, "endColumnIndex": num_cols},
                       "top": {"style": "SOLID", "width": 1}, "bottom": {"style": "SOLID", "width": 1},
                       "left": {"style": "SOLID", "width": 1}, "right": {"style": "SOLID", "width": 1},
                       "innerHorizontal": {"style": "SOLID", "width": 1}, "innerVertical": {"style": "SOLID", "width": 1}}},
    # 2. ヘッダー装飾
    {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": num_cols},
                    "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}, "textFormat": {"bold": True}, "horizontalAlignment": "CENTER"}}, "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"}},
    # 3. 最高単価〜最安単価日の4列を太枠で囲む
    {"updateBorders": {"range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": total_rows, "startColumnIndex": high_idx, "endColumnIndex": low_date_idx + 1},
                       "left": {"style": "SOLID_MEDIUM", "width": 2}, "right": {"style": "SOLID_MEDIUM", "width": 2}}}
]

sh.batch_update({'requests': requests})
print("✅ すべての処理が完了しました。")
