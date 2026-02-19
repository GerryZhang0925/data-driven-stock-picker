# 日本株データ分析システム 使用方法

## 概要

このシステムは、日本株の出来高急増を検知し、ランキングを生成するツールです。実行オプションを使用することで、データ取得の動作を制御できます。

## 基本的な使用方法

### 通常モード（デフォルト）

```bash
python main_jp.py
```

- 必要に応じてデータを自動更新
- 既存データがあれば使用し、不足分のみ取得
- 最新の取引日を確認してデータを更新

### 既存データのみ使用モード

```bash
python main_jp.py --no-download
```

**特徴:**
- データ取得を完全にスキップ
- 既存のCSVファイルのみを使用して計算
- ネットワーク接続が不要
- 高速に実行可能

**使用例:**
- インターネット接続がない環境
- データ取得をせずに既存データで分析したい場合
- バッチ処理で高速に実行したい場合

**注意事項:**
- 既存データがない銘柄はスキップされます
- データが古い場合でも更新されません

### 強制更新モード

```bash
python main_jp.py --force-update
```

**特徴:**
- すべての銘柄のデータを強制的に更新
- 既存データがあっても最新データを取得
- 最新の市場データで分析

**使用例:**
- 定期的に全データを更新したい場合
- データの整合性を確認したい場合
- 最新の市場状況を反映したい場合

**注意事項:**
- 全銘柄のデータ取得には時間がかかります
- ネットワーク接続が必要です

## オプション一覧

| オプション | 説明 | 使用例 |
|-----------|------|--------|
| `--no-download` | データ取得をスキップして既存データのみを使用 | `python main_jp.py --no-download` |
| `--force-update` | 強制的に最新データを取得 | `python main_jp.py --force-update` |
| `--help` | ヘルプを表示 | `python main_jp.py --help` |

## 実行モードの比較

| モード | データ取得 | 既存データ使用 | ネットワーク | 実行速度 |
|--------|----------|--------------|------------|---------|
| 通常 | 必要に応じて | あり | 必要 | 中 |
| `--no-download` | なし | あり | 不要 | 高速 |
| `--force-update` | 常に実行 | なし | 必要 | 低速 |

## 実行例

### 例1: 既存データで高速に分析

```bash
# 既存データのみを使用して分析（ネットワーク接続不要）
python main_jp.py --no-download
```

**出力例:**
```
【モード: 既存データのみ使用】データ取得をスキップして既存データで計算します
保存済み銘柄リストを読み込み: 3700銘柄（更新日時: 2026-02-16 21:04:44）
...
```

### 例2: 最新データで分析

```bash
# すべての銘柄のデータを強制的に更新
python main_jp.py --force-update
```

**出力例:**
```
【モード: 強制更新】すべての銘柄のデータを強制的に更新します
保存済み銘柄リストを読み込み: 3700銘柄（更新日時: 2026-02-16 21:04:44）
...
```

### 例3: ヘルプの表示

```bash
python main_jp.py --help
```

**出力例:**
```
usage: main_jp.py [-h] [--no-download] [--force-update]

日本株の出来高急増検知システム

options:
  -h, --help          show this help message and exit
  --no-download      データ取得をスキップして既存データのみを使用して計算します
  --force-update     強制的に最新データを取得します

使用例:
  python main_jp.py                    # 通常モード（必要に応じてデータを更新）
  python main_jp.py --no-download      # 既存データのみを使用（データ取得をスキップ）
  python main_jp.py --force-update     # 強制更新モード（すべての銘柄のデータを更新）
```

## エラー処理

### オプションの競合

`--no-download`と`--force-update`を同時に指定した場合、エラーが表示されます：

```bash
python main_jp.py --no-download --force-update
```

**出力:**
```
エラー: --no-download と --force-update は同時に指定できません
```

## データの保存場所

- **銘柄リスト**: `data/daily/jp/stock_list_jp.csv`
- **株価データ**: `data/daily/jp/{銘柄コード}.csv`
- **ランキング結果**: `output/volume_spike_ratio_rank.csv`, `output/volume_spike_z_rank.csv`
- **失敗銘柄リスト**: `output/failed_stocks.csv`
- **古いデータ銘柄リスト**: `output/old_data_stocks.csv`

## トラブルシューティング

### 既存データがない場合

`--no-download`オプションを使用した場合、既存データがない銘柄はスキップされます。まず通常モードでデータを取得してください：

```bash
# まずデータを取得
python main_jp.py

# その後、既存データのみで分析
python main_jp.py --no-download
```

### データが古い場合

データを更新したい場合は、`--force-update`オプションを使用してください：

```bash
python main_jp.py --force-update
```

## 関連ファイル

- `main_jp.py`: 日本株用のメインスクリプト
- `jp/data_loader.py`: データ取得モジュール
- `jp/stock_list.py`: 銘柄リスト取得モジュール
- `jp/volume_analyzer.py`: 出来高急増検知モジュール
