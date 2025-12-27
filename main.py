"""
メイン処理
"""
import pandas as pd
from tqdm import tqdm
import os

from config import DATA_DIR, TODAY, MA_WINDOW
from stock_list import get_stock_list
from data_loader import load_or_download, get_latest_trading_date
from volume_analyzer import detect_volume_spike
from output import (
    print_data_acquisition_summary,
    save_failed_stocks,
    save_old_data_stocks,
    retry_failed_stocks,
    save_ranking_results
)


def main():
    """メイン処理"""
    # 銘柄リストを取得
    stocks = get_stock_list()
    
    # 最新取引日を確認
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
    latest_dates = []  # デバッグ用: 各銘柄の最新日を記録
    update_stats = {"success": 0, "failed": 0, "old_data": 0}  # データ更新統計
    failed_stocks = []  # データ取得に失敗した銘柄
    old_data_stocks = []  # 古いデータ（2日以上前）の銘柄
    latest_trading_date_dt = pd.to_datetime(latest_trading_date) if latest_trading_date else None
    # デバッグ用: 銘柄タイプ別の統計
    stats_by_type = {
        "60": {"processed": 0, "has_target_date": 0, "ratio_hit": 0, "z_hit": 0, "failed": 0},
        "68": {"processed": 0, "has_target_date": 0, "ratio_hit": 0, "z_hit": 0, "failed": 0},
        "other": {"processed": 0, "has_target_date": 0, "ratio_hit": 0, "z_hit": 0, "failed": 0}
    }
    
    # 各銘柄を処理
    for _, row in tqdm(stocks.iterrows(), total=len(stocks)):
        code = row["代码"]
        name = row.get("名称", f"銘柄{code}")  # 名称がない場合はコードを使用
        
        # 銘柄タイプを判定
        if code.startswith('60'):
            stock_type = "60"
        elif code.startswith('68'):
            stock_type = "68"
        else:
            stock_type = "other"
        stats_by_type[stock_type]["processed"] += 1

        try:
            # データ取得前の既存データの最新日を確認
            path = f"{DATA_DIR}/{code}.csv"
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
            
            df = load_or_download(code, latest_trading_date=latest_trading_date)
            if df is None:
                failed_stocks.append({"code": code, "name": name, "reason": "データ取得に失敗（既存データなし、新規取得も失敗）"})
                stats_by_type[stock_type]["failed"] += 1
                continue
            if len(df) < MA_WINDOW + 6:
                failed_stocks.append({"code": code, "name": name, "reason": f"データが不足（{len(df)}件、必要: {MA_WINDOW + 6}件以上）"})
                stats_by_type[stock_type]["failed"] += 1
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
            
            # データ更新統計（日付をdatetime型で比較）
            if old_latest_date_dt is None:
                # 新規データ取得
                update_stats["success"] += 1
            elif latest_date_dt > old_latest_date_dt:
                # データが更新された
                update_stats["success"] += 1
            elif latest_date_dt == old_latest_date_dt:
                # 既存データと同じ（更新なし）
                update_stats["old_data"] += 1
            else:
                # データが古くなった（異常）
                update_stats["failed"] += 1

            # 最新取引日のデータでランキングを計算
            latest_trading_date_str = None
            if latest_trading_date:
                latest_trading_date_str = pd.to_datetime(latest_trading_date).strftime("%Y%m%d")
            
            ratio_hit, z_hit, latest = detect_volume_spike(df, target_date_str=latest_trading_date_str)
            
            # 最新取引日のデータがない場合はスキップ
            if latest is None:
                # デバッグ: 60で始まる銘柄で最新取引日のデータがない場合の情報を記録
                if code.startswith('60') and code in ["600838", "603324", "600000", "600001"]:
                    df_sorted = df.sort_values("日期")
                    latest_date_in_df = df_sorted.iloc[-1]["日期"]
                    latest_date_in_df_str = str(latest_date_in_df).replace("-", "")
                    print(f"\n[デバッグ] {code} ({name}): 最新取引日({latest_trading_date_str})のデータなし")
                    print(f"  データの最新日: {latest_date_in_df_str}")
                continue
            
            # 最新取引日のデータがある
            stats_by_type[stock_type]["has_target_date"] += 1
            
            # デバッグ: 最新取引日のデータがあるが条件を満たしていない銘柄を記録（サンプル）
            latest_date_str = str(latest["日期"]).replace("-", "")
            is_target_date = latest_date_str == latest_trading_date_str if latest_trading_date_str else False

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
            
            # デバッグ: 最新取引日のデータがあるが結果に含まれていない銘柄を記録（サンプル）
            if is_target_date and not ratio_hit and not z_hit and code in ["600838", "603324"]:
                vol_mean_val = latest.get("vol_mean", 0)
                z_score_val = latest.get("z_score", 0)
                print(f"\n[デバッグ] {code} ({name}): {latest_trading_date_str}のデータあり、条件未満")
                if vol_mean_val > 0:
                    print(f"  出来高: {latest['成交量']}, 平均: {vol_mean_val:.0f}, 倍率: {latest['成交量']/vol_mean_val:.2f}")
                print(f"  Z-score: {z_score_val:.2f}, 漲跌幅: {latest['涨跌幅']:.2f}, 成交额: {latest.get('成交额', 0):.0f}")

            if ratio_hit:
                ratio_results.append(base)
                stats_by_type[stock_type]["ratio_hit"] += 1

            if z_hit:
                z_results.append(base)
                stats_by_type[stock_type]["z_hit"] += 1

        except Exception as e:
            # エラーを記録
            failed_stocks.append({
                "code": code,
                "name": name,
                "reason": f"エラー: {str(e)[:50]}"
            })
            continue
    
    # 最新日の統計情報を表示（latest_datesが空でも表示）
    if latest_dates:
        print_data_acquisition_summary(latest_dates, update_stats, failed_stocks, old_data_stocks, latest_trading_date_dt, stats_by_type)
    else:
        # latest_datesが空の場合でも統計情報を表示
        print(f"\nデータ取得状況:")
        print(f"今日の日付: {TODAY}")
        print(f"取得された最新取引日: なし（すべての銘柄でデータ取得に失敗した可能性があります）")
        print(f"\nデータ更新統計:")
        print(f"  新規取得/更新成功: {update_stats['success']}銘柄")
        print(f"  既存データ使用（更新なし）: {update_stats['old_data']}銘柄")
        print(f"  更新失敗: {update_stats['failed']}銘柄")
    
    save_failed_stocks(failed_stocks)
    save_old_data_stocks(old_data_stocks)
    if failed_stocks:
        retry_failed_stocks(failed_stocks, lambda code: load_or_download(code, latest_trading_date=latest_trading_date))
    
    # ランキング結果を保存
    save_ranking_results(ratio_results, z_results)


# メイン処理を実行
if __name__ == "__main__":
    main()

