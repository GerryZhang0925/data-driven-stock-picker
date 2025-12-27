"""
出来高急増検知関連
"""
import pandas as pd
from config import MA_WINDOW, VOL_MULTIPLE, Z_THRESHOLD, STD_FLOOR, MIN_PCT_CHG, MIN_AMOUNT


def detect_volume_spike(df, target_date_str=None):
    """
    指定日の倍率法・Z-score法の判定結果を返す
    target_date_strがNoneの場合は最新日を使用
    """
    df = df.sort_values("日期").copy()

    # 日付列を文字列に統一して比較
    df["日期_str"] = df["日期"].astype(str).str.replace("-", "")
    
    # 対象日を決定
    if target_date_str:
        # 指定日と一致するデータを探す
        target_date_normalized = target_date_str.replace("-", "")
        matching_rows = df[df["日期_str"] == target_date_normalized]
        if matching_rows.empty:
            # 指定日のデータがない場合はNoneを返す
            return False, False, None
        target_idx = matching_rows.index[-1]
    else:
        # 最新日を使用
        target_idx = df.index[-1]
    
    # 対象日までのデータで計算
    df_target = df.loc[:target_idx].copy()
    
    if len(df_target) < MA_WINDOW + 1:
        return False, False, None
    
    df_target["vol_mean"] = df_target["成交量"].rolling(MA_WINDOW).mean()
    df_target["vol_std"]  = df_target["成交量"].rolling(MA_WINDOW).std().clip(lower=STD_FLOOR)
    df_target["z_score"]  = (df_target["成交量"] - df_target["vol_mean"]) / df_target["vol_std"]

    latest = df_target.iloc[-1]
    
    # vol_meanがNaNまたは0の場合はスキップ
    if pd.isna(latest["vol_mean"]) or latest["vol_mean"] == 0:
        return False, False, None

    ratio_hit = (
        latest["成交量"] / latest["vol_mean"] >= VOL_MULTIPLE
    )

    z_hit = (
        latest["z_score"] >= Z_THRESHOLD
    )

    common_filter = (
        latest["涨跌幅"] >= MIN_PCT_CHG
        and latest["涨跌幅"] < 9.5
        and latest["成交额"] >= MIN_AMOUNT
    )

    return ratio_hit and common_filter, z_hit and common_filter, latest


def calc_forward_return(df, days):
    """
    最新日からdays後の終値リターン
    """
    df = df.sort_values("日期")
    if len(df) < days + 1:
        return None

    entry = df.iloc[-1]["收盘"]
    future = df.iloc[-1 + days]["收盘"]

    return (future - entry) / entry

