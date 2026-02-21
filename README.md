# supplier_analysis

BigQueryの仕入データを集計してGoogle Sheetsに出力する分析スクリプト。

非公開。問い合わせは森口大輔まで。

---

## 概要

`logistics-449115.lastmile.supplyAcquisition` テーブルから直近14日間の仕入データを取得し、
商品コード別・仕入先別に単価統計と短期トレンドを集計してGoogle Sheetsに書き込む。

---

## 出力内容

| 列 | 内容 |
|----|------|
| 商品コード / 商品名 | 基本情報 |
| 最高単価 / 最安単価 | 直近14日の高値・安値と日付 |
| 平均単価 | 直近14日の平均 |
| 短期トレンド | 直近7日 vs 14日の比較（UP / DOWN / FLAT） |
| 仕入先N / 単価 / 取引回数 | 仕入先別の統計（複数列） |

---

## 実行環境

Cloud Run または Cloud Scheduler からの定期実行を想定。

**環境変数**:
- `GCP_SA_KEY` — サービスアカウントJSONを文字列で渡す

---

## 依存

```bash
pip install pandas numpy gspread google-auth google-cloud-bigquery
```

---

## 設計思想

BigQuery → pandas で集計 → Google Sheets に書き込む一方向パイプライン。
フロントエンドなし。Sheetsが表示層を担う。
