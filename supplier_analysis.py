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
# 1. 認証設定(GitHub Secretsから取得)
# ============================================================
env_key = os.getenv("GCP_SA_KEY")

if not env_key:
    raise ValueError("GCP_SA_KEY is not set")

info = json.loads(env_key)

# 鍵の種類を自動判別
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
today = pd.Timestamp(datetime.now() + timedelta(hours=9)).normalize()

# ============================================================
# 3. BigQueryからデータ取得
# ============================================================
sql = """
SELECT *
FROM `logistics-449115.lastmile.supplyAcquisition`
"""
print("BigQueryから仕入データを取得中...")
df = client.query(sql).to_dataframe()

# ============================================================
# 4. 日付パース
# ============================================================
df['invoiceDate_parsed'] = pd.to_datetime(
    df['invoiceDate'].astype(str),
    format='%Y%m%d',
    errors='coerce'
)

max_date = df['invoiceDate_parsed'].max()

# ============================================================
# 5. 直近2週間フィルタ
# ============================================================
df_2weeks = df[
    (df['invoiceDate_parsed'] >= max_date - timedelta(days=14)) &
    (df['unitPrice'] > 0) &
    (df['kgAmount'] > 0) &
    (df['itemCode'].notna()) &
    (df['itemCode'].astype(str) != '') &
    (df['supplierName1'].notna()) &
    (df['supplierName1'] != '')
].copy()

df_2weeks = df_2weeks[np.isfinite(df_2weeks['unitPrice'])]

print(f"📦 抽出データ件数（2週間）: {len(df_2weeks)}件")

# ============================================================
# 6. 商品ごとの標準仕入れ単位（最頻値）
# ============================================================
standard_unit_map = (
    df_2weeks
    .groupby('itemCode')['kgAmount']
    .agg(lambda x: x.mode().iloc[0] if not x.mode().empty else x.iloc[0])
    .to_dict()
)

df_2weeks['standard_unit'] = df_2weeks['itemCode'].map(standard_unit_map)
df_2weeks = df_2weeks[df_2weeks['kgAmount'] == df_2weeks['standard_unit']].copy()

print(f"📐 仕入単位統一後件数: {len(df_2weeks)}件")

# ============================================================
# 7. 商品マスタ情報
# ============================================================
item_info = (
    df_2weeks
    .groupby('itemCode')
    .agg({
        'itemName': 'first',
        'standard_unit': 'first'
    })
    .reset_index()
)

# ============================================================
# 8. 単価集計（全体 / 短期）
# ============================================================
overall_avg = df_2weeks.groupby('itemCode')['unitPrice'].mean().round(0)

df_short = df_2weeks[
    df_2weeks['invoiceDate_parsed'] >= max_date - timedelta(days=7)
]
short_avg = df_short.groupby('itemCode')['unitPrice'].mean().round(0)

# ============================================================
# 9. 短期トレンド判定
# ============================================================
def judge_trend(item_code):
    overall = overall_avg.get(item_code)
    short = short_avg.get(item_code)

    if overall is None or short is None:
        return 'FLAT'

    if short > overall:
        return 'UP'
    elif short < overall:
        return 'DOWN'
    else:
        return 'FLAT'

# ============================================================
# 10. サプライヤー別 単価統計
# ============================================================
supplier_stats = (
    df_2weeks
    .groupby(['itemCode', 'supplierName1'])
    .agg(
        max_unit_price=('unitPrice', 'max'),
        min_unit_price=('unitPrice', 'min'),
        avg_unit_price=('unitPrice', 'mean'),
        sample_count=('unitPrice', 'count')
    )
    .round(0)
    .reset_index()
)

supplier_stats = supplier_stats[
    np.isfinite(supplier_stats['avg_unit_price'])
]

supplier_stats = supplier_stats.sort_values(
    ['itemCode', 'avg_unit_price']
)

# ============================================================
# 11. 商品別 全体最高・最安単価
# ============================================================
overall_highest = df_2weeks.groupby('itemCode')['unitPrice'].max().round(0)
overall_lowest = df_2weeks.groupby('itemCode')['unitPrice'].min().round(0)

# ============================================================
# 12. ワイド形式作成
# ============================================================
print("ワイド形式でデータを作成中...")
result_rows = []

for _, item in item_info.iterrows():
    item_code = item['itemCode']
    item_name = item['itemName']
    unit = item['standard_unit']

    suppliers = supplier_stats[
        supplier_stats['itemCode'] == item_code
    ].reset_index(drop=True)

    if suppliers.empty:
        continue

    overall = overall_avg.get(item_code)
    highest = overall_highest.get(item_code)
    lowest = overall_lowest.get(item_code)

    if not np.isfinite(overall):
        continue

    row = {
        '商品コード': item_code,
        '商品名': item_name,
        '仕入れ単位': f'{unit}kg',
        '最高単価': int(highest),
        '最安単価': int(lowest),
        '平均単価': int(overall),
        '短期トレンド': judge_trend(item_code)
    }

    for idx, s in suppliers.iterrows():
        rank = idx + 1
        row[f'仕入先{rank}'] = s['supplierName1']
        row[f'仕入先{rank}_単価'] = (
            f"{int(s['max_unit_price'])}/"
            f"{int(s['min_unit_price'])}："
            f"{int(s['avg_unit_price'])}"
        )
        row[f'仕入先{rank}_取引回数'] = int(s['sample_count'])

    result_rows.append(row)

result = pd.DataFrame(result_rows).fillna('')

# ============================================================
# 13. 集計期間
# ============================================================
start_date = df_2weeks['invoiceDate_parsed'].min().strftime('%Y/%m/%d')
end_date = df_2weeks['invoiceDate_parsed'].max().strftime('%Y/%m/%d')

result['集計期間'] = f'{start_date} - {end_date}'

base_cols = [
    '商品コード', '商品名', '仕入れ単位',
    '最高単価', '最安単価', '平均単価',
    '短期トレンド', '集計期間'
]

supplier_cols = sorted(
    [c for c in result.columns if c.startswith('仕入先')]
)

result = result[base_cols + supplier_cols]

print(f"📊 完成データ件数: {len(result)}件")
print(f"📅 集計期間: {start_date} - {end_date}")

# ============================================================
# 14. Google Sheets 出力
# ============================================================
print("スプレッドシートを更新中...")
sh = gc.open_by_key(SPREADSHEET_ID)
sheet_name = '仕入先分析_単価'

try:
    worksheet = sh.worksheet(sheet_name)
except gspread.exceptions.WorksheetNotFound:
    worksheet = sh.add_worksheet(
        title=sheet_name,
        rows=1000,
        cols=60
    )

worksheet.clear()
worksheet.update(
    [result.columns.tolist()] + result.values.tolist(),
    value_input_option='RAW'
)

print("✅ Google Sheets出力完了")
print(f"🔗 https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")
print("すべての更新処理が完了しました。")
