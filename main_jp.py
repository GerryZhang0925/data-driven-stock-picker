"""
日本株の出来高急増検知システム
"""
import argparse
import pandas as pd
from tqdm import tqdm
import os

from config import DATA_DIR_JP, OUTPUT_DIR, TODAY, MA_WINDOW
# 日本株用のモジュール
from jp.stock_list import get_stock_list
from jp.data_loader import load_or_download, get_latest_trading_date
from jp.volume_analyzer import detect_volume_spike

from output import (
    print_data_acquisition_summary,
    save_failed_stocks,
    save_old_data_stocks,
    retry_failed_stocks,
    save_ranking_results
)

# 収益バリュー株ランキング用の設定
CONFIG_FILE_JP = "value_stock_screening_config.json"
OUTPUT_FILE_VALUE_JP = f"{OUTPUT_DIR}/value_stock_ranking_jp.csv"
FUNDAMENTALS_PATH_JP = f"{DATA_DIR_JP}/fundamentals_jp.csv"


def load_screening_config(config_file=CONFIG_FILE_JP):
    """
    スクリーニング条件をJSONファイルから読み込む
    """
    if not os.path.exists(config_file):
        return {
            "operating_margin_min": 10.0,
            "per_max": 10.0,
            "pbr_max": 1.5,
            "roa_min": 7.0,
            "market_cap_max": 30000000000  # 日本株は300億円
        }
    
    try:
        import json
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        return config
    except Exception as e:
        print(f"設定ファイルの読み込みに失敗: {str(e)}")
        return {
            "operating_margin_min": 10.0,
            "per_max": 10.0,
            "pbr_max": 1.5,
            "roa_min": 7.0,
            "market_cap_max": 30000000000
        }


def screen_value_stocks(fundamentals_df, config):
    """
    ファンダメンタル情報から収益バリュー株をスクリーニング
    """
    if fundamentals_df is None or fundamentals_df.empty:
        print("エラー: ファンダメンタル情報がありません")
        return None
    
    # 数値列を数値型に変換
    numeric_columns = ['PER', 'PBR', '営業利益率', 'ROA', 'ROE', '時価総額', '配当利回り', 'EPS']
    for col in numeric_columns:
        if col in fundamentals_df.columns:
            fundamentals_df[col] = pd.to_numeric(fundamentals_df[col], errors='coerce')
    
    filtered_df = fundamentals_df.copy()
    
    # 営業利益率
    if '営業利益率' in filtered_df.columns:
        filtered_df = filtered_df[
            (filtered_df['営業利益率'].notna()) & 
            (filtered_df['営業利益率'] >= config.get('operating_margin_min', 10.0))
        ]
        print(f"営業利益率 {config.get('operating_margin_min', 10.0)}%以上: {len(filtered_df)}銘柄")
    
    # PER
    if 'PER' in filtered_df.columns:
        filtered_df = filtered_df[
            (filtered_df['PER'].notna()) & 
            (filtered_df['PER'] > 0) & 
            (filtered_df['PER'] <= config.get('per_max', 10.0))
        ]
        print(f"PER {config.get('per_max', 10.0)}倍以下: {len(filtered_df)}銘柄")
    
    # PBR
    if 'PBR' in filtered_df.columns:
        filtered_df = filtered_df[
            (filtered_df['PBR'].notna()) & 
            (filtered_df['PBR'] > 0) & 
            (filtered_df['PBR'] <= config.get('pbr_max', 1.5))
        ]
        print(f"PBR {config.get('pbr_max', 1.5)}倍以下: {len(filtered_df)}銘柄")
    
    # ROA（取得できる場合）
    if 'ROA' in filtered_df.columns:
        filtered_df = filtered_df[
            (filtered_df['ROA'].notna()) & 
            (filtered_df['ROA'] >= config.get('roa_min', 7.0))
        ]
        print(f"ROA {config.get('roa_min', 7.0)}%以上: {len(filtered_df)}銘柄")
    
    # 時価総額
    if '時価総額' in filtered_df.columns:
        market_cap_max = config.get('market_cap_max', 30000000000)
        filtered_df = filtered_df[
            (filtered_df['時価総額'].notna()) & 
            (filtered_df['時価総額'] > 0) & 
            (filtered_df['時価総額'] <= market_cap_max)
        ]
        print(f"時価総額 {market_cap_max / 1e8:.0f}億円以下: {len(filtered_df)}銘柄")
    
    return filtered_df


def create_ranking(df):
    """
    スクリーニング結果をランキング形式に変換
    """
    if df is None or df.empty:
        return None
    
    # ランキング用の列を選択（必須列 + オプション列）
    ranking_columns = ['代码', '名称']
    
    # 必須で含める列（存在する場合）
    required_cols = ['PER', 'PBR', '時価総額']
    for col in required_cols:
        if col in df.columns:
            ranking_columns.append(col)
    
    # オプション列（存在する場合に追加）
    optional_cols = ['営業利益率', 'ROA', 'ROE', 'EPS', '配当利回り']
    for col in optional_cols:
        if col in df.columns:
            ranking_columns.append(col)
    
    ranking_df = df[ranking_columns].copy()
    
    # 時価総額を億円単位に変換（表示用）
    if '時価総額' in ranking_df.columns:
        ranking_df['時価総額（億円）'] = (ranking_df['時価総額'] / 1e8).round(2)
    
    # 数値列のフォーマット（小数点以下2桁に統一）
    numeric_cols = ['営業利益率', 'ROA', 'ROE', '配当利回り', 'PER', 'PBR', 'EPS']
    for col in numeric_cols:
        if col in ranking_df.columns:
            ranking_df[col] = pd.to_numeric(ranking_df[col], errors='coerce').round(2)
    
    # 複合スコアを計算
    if all(col in ranking_df.columns for col in ['営業利益率', 'ROA', 'PER', 'PBR']):
        ranking_df['バリュースコア'] = (
            (ranking_df['営業利益率'] + ranking_df['ROA']) / 
            (ranking_df['PER'] + ranking_df['PBR'] + 0.01) * 100
        ).round(2)
        ranking_df = ranking_df.sort_values('バリュースコア', ascending=False)
    else:
        # スコアが計算できない場合はPERでソート
        if 'PER' in ranking_df.columns:
            ranking_df = ranking_df.sort_values('PER')
    
    return ranking_df


def generate_value_stock_ranking():
    """
    収益バリュー株ランキングを生成
    """
    from jp.fundamentals import load_fundamentals
    from jp.stock_list import STOCK_LIST_PATH_JP
    
    print("\n" + "="*50)
    print("=== 日本株 収益バリュー株ランキング生成 ===")
    print("="*50 + "\n")
    
    # 設定ファイルを読み込み
    config = load_screening_config()
    print(f"スクリーニング条件:")
    print(f"  営業利益率: {config.get('operating_margin_min', 10.0)}%以上")
    print(f"  PER: {config.get('per_max', 10.0)}倍以下")
    print(f"  PBR: {config.get('pbr_max', 1.5)}倍以下")
    print(f"  ROA: {config.get('roa_min', 7.0)}%以上")
    print(f"  時価総額: {config.get('market_cap_max', 30000000000) / 1e8:.0f}億円以下\n")
    
    # ファンダメンタル情報を読み込み
    print("ファンダメンタル情報を読み込み中...")
    fundamentals_df = load_fundamentals()
    
    if fundamentals_df is None or fundamentals_df.empty:
        print("エラー: ファンダメンタル情報が取得できませんでした。")
        print("先に get_fundamentals_jp.py を実行してファンダメンタル情報を取得してください。")
        return
    
    print(f"読み込み完了: {len(fundamentals_df)}銘柄")
    
    # 日本語名を取得（stock_list_jp.csvから）
    if os.path.exists(STOCK_LIST_PATH_JP):
        try:
            stock_list_df = pd.read_csv(STOCK_LIST_PATH_JP, encoding="utf-8-sig")
            if "代码" in stock_list_df.columns and "名称" in stock_list_df.columns:
                # コード列を文字列型に統一
                stock_list_df["代码"] = stock_list_df["代码"].astype(str)
                fundamentals_df["代码"] = fundamentals_df["代码"].astype(str)
                
                # 銘柄リストの日本語名でマージ
                fundamentals_df = fundamentals_df.merge(
                    stock_list_df[["代码", "名称"]],
                    on="代码",
                    how="left",
                    suffixes=("", "_jp")
                )
                
                # 日本語名がある場合は優先的に使用
                if "名称_jp" in fundamentals_df.columns:
                    # 日本語名が存在する場合はそれを使用、なければ元の名称を使用
                    fundamentals_df["名称"] = fundamentals_df["名称_jp"].fillna(fundamentals_df["名称"])
                    fundamentals_df = fundamentals_df.drop("名称_jp", axis=1)
        except Exception as e:
            print(f"警告: 日本語名の取得に失敗しました: {str(e)}")
    
    # データの有無を確認
    if 'PER' in fundamentals_df.columns:
        per_count = fundamentals_df['PER'].notna().sum()
        print(f"  PERデータあり: {per_count}銘柄")
    if 'PBR' in fundamentals_df.columns:
        pbr_count = fundamentals_df['PBR'].notna().sum()
        print(f"  PBRデータあり: {pbr_count}銘柄")
    if '営業利益率' in fundamentals_df.columns:
        op_margin_count = fundamentals_df['営業利益率'].notna().sum()
        print(f"  営業利益率データあり: {op_margin_count}銘柄")
    if 'ROA' in fundamentals_df.columns:
        roa_count = fundamentals_df['ROA'].notna().sum()
        print(f"  ROAデータあり: {roa_count}銘柄")
    print()
    
    # スクリーニング実行
    print("スクリーニング実行中...")
    screened_df = screen_value_stocks(fundamentals_df, config)
    
    if screened_df is None or screened_df.empty:
        print("\n条件に合う銘柄が見つかりませんでした。")
        return
    
    print(f"\nスクリーニング結果: {len(screened_df)}銘柄が条件を満たしました。\n")
    
    # ランキング作成
    ranking_df = create_ranking(screened_df)
    
    if ranking_df is None or ranking_df.empty:
        print("エラー: ランキングの作成に失敗しました。")
        return
    
    # CSVに保存
    ranking_df.to_csv(OUTPUT_FILE_VALUE_JP, index=False, encoding="utf-8-sig")
    print(f"ランキングを保存しました: {OUTPUT_FILE_VALUE_JP}")
    print(f"  対象銘柄数: {len(ranking_df)}銘柄\n")
    
    # 上位10銘柄を表示
    print("【収益バリュー株ランキング 上位10銘柄】")
    display_df = ranking_df.head(10).copy()
    # 時価総額列を削除（億円版のみ表示）
    if '時価総額' in display_df.columns and '時価総額（億円）' in display_df.columns:
        display_df = display_df.drop('時価総額', axis=1)
    print(display_df.to_string(index=False))


def main(no_download=False, force_update=False):
    """メイン処理"""
    # オプション表示
    if no_download:
        print("【モード: 既存データのみ使用】データ取得をスキップして既存データで計算します")
    elif force_update:
        print("【モード: 強制更新】すべての銘柄のデータを強制的に更新します")
    
    # 銘柄リストを取得
    stocks = get_stock_list(no_download=no_download)
    
    # 最新取引日を確認（no_downloadモードの場合はスキップ）
    latest_trading_date = None
    if not no_download:
        latest_trading_date = get_latest_trading_date(stocks)
    if latest_trading_date:
        print(f"最新取引日（確認）: {latest_trading_date}")
        # 日付を比較可能な形式に変換
        latest_date_str = pd.to_datetime(latest_trading_date).strftime("%Y%m%d")
        if latest_date_str < TODAY:
            print(f"注意: 今日({TODAY})は取引日ではない可能性があります。最新の取引日は {latest_trading_date} です。")
    print(f"今日の日付: {TODAY}\n")
    
    # 統計用変数の初期化
    ratio_results = []
    z_results = []
    latest_dates = []
    update_stats = {"success": 0, "failed": 0, "old_data": 0}
    failed_stocks = []
    old_data_stocks = []
    latest_trading_date_dt = pd.to_datetime(latest_trading_date) if latest_trading_date else None
    stats_by_type = {
        "jp": {"processed": 0, "has_target_date": 0, "ratio_hit": 0, "z_hit": 0, "failed": 0}
    }
    
    # 各銘柄を処理
    for _, row in tqdm(stocks.iterrows(), total=len(stocks)):
        code = row["代码"]
        name = row.get("名称", f"銘柄{code}")
        stats_by_type["jp"]["processed"] += 1

        try:
            # データ取得前の既存データの最新日を確認
            path = f"{DATA_DIR_JP}/{code}.csv"
            old_latest_date = None
            old_latest_date_dt = None
            if os.path.exists(path):
                try:
                    df_check = pd.read_csv(path)
                    if "日期" in df_check.columns and not df_check.empty:
                        old_latest_date = df_check["日期"].max()
                        old_latest_date_dt = pd.to_datetime(old_latest_date)
                except Exception:
                    pass
            
            # データ取得（no_downloadモードの場合は既存データのみ使用）
            if no_download:
                # 既存データのみを読み込む
                if os.path.exists(path):
                    try:
                        df = pd.read_csv(path, encoding="utf-8-sig")
                        # 日付列を正規化
                        if "日期" in df.columns:
                            df["日期"] = pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d")
                    except Exception as e:
                        failed_stocks.append({"code": code, "name": name, "reason": f"既存データの読み込みに失敗: {str(e)[:50]}"})
                        stats_by_type["jp"]["failed"] += 1
                        continue
                else:
                    failed_stocks.append({"code": code, "name": name, "reason": "既存データが存在しません"})
                    stats_by_type["jp"]["failed"] += 1
                    continue
            else:
                df = load_or_download(code, latest_trading_date=latest_trading_date, force_update=force_update)
            
            if df is None:
                failed_stocks.append({"code": code, "name": name, "reason": "データ取得に失敗（既存データなし、新規取得も失敗）"})
                stats_by_type["jp"]["failed"] += 1
                continue
            if len(df) < MA_WINDOW + 6:
                failed_stocks.append({"code": code, "name": name, "reason": f"データが不足（{len(df)}件、必要: {MA_WINDOW + 6}件以上）"})
                stats_by_type["jp"]["failed"] += 1
                continue

            # 最新日を記録
            df_sorted = df.sort_values("日期")
            latest_date = df_sorted.iloc[-1]["日期"]
            latest_dates.append(latest_date)
            latest_date_dt = pd.to_datetime(latest_date)
            
            # 最新日が古いかチェック（2日以上前）
            if latest_trading_date_dt:
                days_behind = (latest_trading_date_dt - latest_date_dt).days
                if days_behind >= 2:
                    old_data_stocks.append({
                        "code": code,
                        "name": name,
                        "latest_date": latest_date,
                        "days_behind": days_behind
                    })
            
            # データ更新統計
            if old_latest_date_dt is None:
                update_stats["success"] += 1
            elif latest_date_dt > old_latest_date_dt:
                update_stats["success"] += 1
            elif latest_date_dt == old_latest_date_dt:
                update_stats["old_data"] += 1
            else:
                update_stats["failed"] += 1

            # 最新取引日のデータでランキングを計算
            latest_trading_date_str = None
            if latest_trading_date:
                latest_trading_date_str = pd.to_datetime(latest_trading_date).strftime("%Y%m%d")
            
            ratio_hit, z_hit, latest = detect_volume_spike(df, target_date_str=latest_trading_date_str)
            
            # 最新取引日のデータがない場合はスキップ
            if latest is None:
                continue
            
            # 最新取引日のデータがある
            stats_by_type["jp"]["has_target_date"] += 1

            base = {
                "code": code,
                "name": name,
                "date": latest["日期"],
                "close": latest["收盘"],
                "pct_chg": latest["涨跌幅"],
                "volume": latest["成交量"],
                "z_score": round(latest["z_score"], 2),
                "vol_ratio": round(latest["成交量"] / latest["vol_mean"], 2),
            }

            if ratio_hit:
                ratio_results.append(base)
                stats_by_type["jp"]["ratio_hit"] += 1

            if z_hit:
                z_results.append(base)
                stats_by_type["jp"]["z_hit"] += 1

        except Exception as e:
            failed_stocks.append({
                "code": code,
                "name": name,
                "reason": f"エラー: {str(e)[:50]}"
            })
            stats_by_type["jp"]["failed"] += 1
            continue
    
    # 統計情報を表示
    if latest_dates:
        print_data_acquisition_summary(latest_dates, update_stats, failed_stocks, old_data_stocks, latest_trading_date_dt, stats_by_type)
    else:
        print(f"\nデータ取得状況:")
        print(f"今日の日付: {TODAY}")
        print(f"取得された最新取引日: なし")
        print(f"\nデータ更新統計:")
        print(f"  新規取得/更新成功: {update_stats['success']}銘柄")
        print(f"  既存データ使用（更新なし）: {update_stats['old_data']}銘柄")
        print(f"  更新失敗: {update_stats['failed']}銘柄")
    
    save_failed_stocks(failed_stocks)
    save_old_data_stocks(old_data_stocks)
    if failed_stocks and not no_download:
        retry_failed_stocks(failed_stocks, lambda code: load_or_download(code, latest_trading_date=latest_trading_date, force_update=force_update))
    
    # ランキング結果を保存
    save_ranking_results(ratio_results, z_results)
    
    # 収益バリュー株ランキングを生成
    generate_value_stock_ranking()


# メイン処理を実行
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="日本株の出来高急増検知システム",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  python main_jp.py                    # 通常モード（必要に応じてデータを更新）
  python main_jp.py --no-download      # 既存データのみを使用（データ取得をスキップ）
  python main_jp.py --force-update     # 強制更新モード（すべての銘柄のデータを更新）
  python main_jp.py --help             # このヘルプメッセージを表示

オプションの説明:
  --no-download
    データ取得をスキップして既存データのみを使用して計算します。
    ネットワーク接続が不要で、高速に実行できます。
    既存データがない銘柄はスキップされます。

  --force-update
    すべての銘柄のデータを強制的に更新します。
    既存データがあっても最新データを取得します。
    全銘柄のデータ取得には時間がかかります。

  --help, -h
    このヘルプメッセージを表示して終了します。

注意事項:
  --no-download と --force-update は同時に指定できません。
        """,
        add_help=True
    )
    parser.add_argument(
        "--no-download",
        action="store_true",
        help="データ取得をスキップして既存データのみを使用して計算します"
    )
    parser.add_argument(
        "--force-update",
        action="store_true",
        help="強制的に最新データを取得します"
    )
    
    try:
        args = parser.parse_args()
    except SystemExit as e:
        # 不正なオプション指定時はヘルプを表示して終了
        # SystemExit(2)は通常の--help表示、SystemExit(0)は正常終了
        if e.code != 0:
            parser.print_help()
        exit(e.code if e.code is not None else 1)
    except Exception as e:
        # その他のエラー時もヘルプを表示
        print(f"エラー: {str(e)}")
        print("\n使用可能なオプション:")
        parser.print_help()
        exit(1)
    
    # オプションの競合チェック
    if args.no_download and args.force_update:
        print("エラー: --no-download と --force-update は同時に指定できません")
        print("\n使用可能なオプション:")
        parser.print_help()
        exit(1)
    
    main(no_download=args.no_download, force_update=args.force_update)
