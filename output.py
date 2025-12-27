"""
出力・表示関連
"""
import pandas as pd
from config import TODAY, OUTPUT_DIR


def print_data_acquisition_summary(latest_dates, update_stats, failed_stocks, old_data_stocks, latest_trading_date_dt, stats_by_type):
    """データ取得状況のサマリーを表示"""
    if not latest_dates:
        return
    
    unique_dates = pd.Series(latest_dates).unique()
    print(f"\nデータ取得状況:")
    print(f"今日の日付: {TODAY}")
    print(f"取得された最新取引日: {sorted(unique_dates, reverse=True)[:5]}")  # 最新5日を表示
    print(f"\nデータ更新統計:")
    print(f"  新規取得/更新成功: {update_stats['success']}銘柄")
    print(f"  既存データ使用（更新なし）: {update_stats['old_data']}銘柄")
    print(f"  更新失敗: {update_stats['failed']}銘柄")
    
    # 12月25日のデータが取得できた銘柄数を確認
    target_date_str = "2025-12-25"
    count_20251225 = sum(1 for d in latest_dates if str(d) == target_date_str or str(d).replace("-", "") == "20251225")
    print(f"  12月25日のデータ取得済み: {count_20251225}銘柄")
    
    if len(unique_dates) > 1:
        print(f"\n警告: 複数の最新日が検出されました。一部の銘柄でデータ取得が失敗している可能性があります。")
        if failed_stocks or old_data_stocks:
            print(f"  詳細は上記の失敗銘柄リストと古いデータ銘柄リストを確認してください。")
    
    # 銘柄タイプ別の統計を表示
    print(f"\n銘柄タイプ別の処理統計:")
    for stock_type, stats in stats_by_type.items():
        type_name = {"60": "上海A株（60）", "68": "科創板（68）", "other": "その他"}[stock_type]
        print(f"  {type_name}:")
        print(f"    処理済み: {stats['processed']}銘柄")
        print(f"    最新取引日データあり: {stats['has_target_date']}銘柄")
        print(f"    Ratio条件適合: {stats['ratio_hit']}銘柄")
        print(f"    Z-score条件適合: {stats['z_hit']}銘柄")
        print(f"    失敗: {stats['failed']}銘柄")


def save_failed_stocks(failed_stocks):
    """失敗した銘柄の情報を表示してCSVに保存"""
    if not failed_stocks:
        return
    
    print(f"\nデータ取得に失敗した銘柄: {len(failed_stocks)}銘柄")
    if len(failed_stocks) <= 20:
        for stock in failed_stocks[:20]:
            print(f"  {stock['code']} ({stock['name']}): {stock['reason']}")
    else:
        for stock in failed_stocks[:10]:
            print(f"  {stock['code']} ({stock['name']}): {stock['reason']}")
        print(f"  ... 他 {len(failed_stocks) - 10}銘柄")
    
    # 失敗した銘柄をCSVに保存
    failed_df = pd.DataFrame(failed_stocks)
    failed_path = f"{OUTPUT_DIR}/failed_stocks.csv"
    failed_df.to_csv(failed_path, index=False, encoding="utf-8-sig")
    print(f"  失敗銘柄リストを保存: {failed_path}")


def save_old_data_stocks(old_data_stocks):
    """古いデータの銘柄の情報を表示してCSVに保存"""
    if not old_data_stocks:
        return
    
    print(f"\n古いデータ（2日以上前）の銘柄: {len(old_data_stocks)}銘柄")
    # 日数でソート
    old_data_stocks_sorted = sorted(old_data_stocks, key=lambda x: x['days_behind'], reverse=True)
    if len(old_data_stocks_sorted) <= 20:
        for stock in old_data_stocks_sorted[:20]:
            print(f"  {stock['code']} ({stock['name']}): 最新日={stock['latest_date']}, {stock['days_behind']}日前")
    else:
        for stock in old_data_stocks_sorted[:10]:
            print(f"  {stock['code']} ({stock['name']}): 最新日={stock['latest_date']}, {stock['days_behind']}日前")
        print(f"  ... 他 {len(old_data_stocks_sorted) - 10}銘柄")
    
    # 古いデータの銘柄をCSVに保存
    old_data_df = pd.DataFrame(old_data_stocks_sorted)
    old_data_path = f"{OUTPUT_DIR}/old_data_stocks.csv"
    old_data_df.to_csv(old_data_path, index=False, encoding="utf-8-sig")
    print(f"  古いデータ銘柄リストを保存: {old_data_path}")


def retry_failed_stocks(failed_stocks, load_or_download_func):
    """失敗した銘柄の再試行"""
    if not failed_stocks or len(failed_stocks) == 0:
        return
    
    print(f"\n失敗した銘柄の再試行を実行しますか？ (y/n): ", end="")
    retry_choice = input().strip().lower()
    if retry_choice != 'y':
        return
    
    print(f"\n失敗した{len(failed_stocks)}銘柄を再試行中...")
    retry_success = 0
    retry_failed = []
    from tqdm import tqdm
    from config import MA_WINDOW
    for stock in tqdm(failed_stocks, desc="再試行"):
        try:
            df = load_or_download_func(stock['code'])
            if df is not None and len(df) >= MA_WINDOW + 6:
                retry_success += 1
            else:
                retry_failed.append(stock)
        except Exception as e:
            retry_failed.append({**stock, "reason": f"再試行エラー: {str(e)[:50]}"})
    
    print(f"\n再試行結果:")
    print(f"  成功: {retry_success}銘柄")
    print(f"  失敗: {len(retry_failed)}銘柄")
    if retry_failed:
        retry_failed_df = pd.DataFrame(retry_failed)
        retry_failed_path = f"{OUTPUT_DIR}/retry_failed_stocks.csv"
        retry_failed_df.to_csv(retry_failed_path, index=False, encoding="utf-8-sig")
        print(f"  再試行失敗銘柄リストを保存: {retry_failed_path}")


def save_ranking_results(ratio_results, z_results):
    """ランキング結果をCSVに保存して表示"""
    # Ratioランキング
    if ratio_results:
        ratio_rank_df = pd.DataFrame(ratio_results).sort_values("vol_ratio", ascending=False)
        rank_path = f"{OUTPUT_DIR}/volume_spike_ratio_rank.csv"
        ratio_rank_df.to_csv(rank_path, index=False, encoding="utf-8-sig")
        print("出来高急増ランキング 上位10銘柄 (Ratio)")
        print(ratio_rank_df.head(10))
    else:
        ratio_rank_df = pd.DataFrame()
        print("出来高急増ランキング (Ratio): 該当銘柄なし")

    # Z-scoreランキング
    if z_results:
        z_rank_df = pd.DataFrame(z_results).sort_values("z_score", ascending=False)
        rank_path = f"{OUTPUT_DIR}/volume_spike_z_rank.csv"
        z_rank_df.to_csv(rank_path, index=False, encoding="utf-8-sig")
        print("出来高急増ランキング 上位10銘柄 (Z)")
        print(z_rank_df.head(10))
    else:
        z_rank_df = pd.DataFrame()
        print("出来高急増ランキング (Z): 該当銘柄なし")

