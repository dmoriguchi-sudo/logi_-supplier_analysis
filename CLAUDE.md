# CLAUDE.md - supplier_analysis

## プロジェクト概要
仕入先（サプライヤー）の週次分析を自動実行するバッチ処理。
BigQueryとGoogleスプレッドシートのデータをもとに分析を行い、毎週金曜日に自動実行される。

## 技術スタック
- Python 3.11
- Google Cloud BigQuery
- Google Sheets API（gspread）
- GitHub Actions（週次スケジュール実行）

## 実行スケジュール
- 毎週金曜日 24:00 JST（UTC 15:00）
- 手動実行も可能（workflow_dispatch）

## 使用シークレット
| シークレット名 | 内容 |
|---|---|
| `GCP_SA_KEY` | GCPサービスアカウントのJSONキー |

## 主要ファイル
| ファイル | 役割 |
|---|---|
| `supplier_analysis.py` | メイン分析スクリプト |
| `.github/workflows/weekly_supplier_analysis.yml` | GitHub Actionsワークフロー |

## GCP設定
- プロジェクト: `logistics-449115`
- スプレッドシートID: `1w-Kknr-Or8zwpL8SUmnhsev-mrHuDr2YLhAzjDVRh-4`
- 認証: サービスアカウント（BigQuery + Sheets APIのスコープ）

## ローカル実行方法
```bash
export GCP_SA_KEY='（サービスアカウントJSONの内容）'
pip install pandas numpy gspread google-auth google-cloud-bigquery db-dtypes pyarrow
python supplier_analysis.py
```

## 注意事項
- GCP_SA_KEY はJSON文字列をそのまま環境変数に設定する
- dmoriguchi-sudo側のActionsが有効（vegekul側は無効）
