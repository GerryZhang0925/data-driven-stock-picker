"""
データ取得・ダウンロード関連
"""
import akshare as ak
import pandas as pd
import os
import time
from config import DATA_DIR, TODAY, DEFAULT_START


def _normalize_date_column(df):
    """日付列を文字列型に統一"""
    if df is None or df.empty or "日期" not in df.columns:
        return df
    try:
        if df["日期"].dtype != 'object' or pd.api.types.is_datetime64_any_dtype(df["日期"]):
            df["日期"] = pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d")
        else:
            df["日期"] = df["日期"].astype(str).str.replace("/", "-")
    except Exception:
        pass
    return df


def _load_existing_data(code, path):
    """既存データの読み込みと日付の正規化、days_oldの計算"""
    if not os.path.exists(path):
        return None, 0
    
    df_old = pd.read_csv(path)
    df_old = _normalize_date_column(df_old)
    
    last_date = df_old["日期"].max()
    last_date_dt = pd.to_datetime(last_date)
    days_old = (pd.to_datetime(TODAY) - last_date_dt).days
    
    if code in ["600838", "603324"]:
        print(f"\n[デバッグ開始] {code}:")
        print(f"  既存データの最新日: {last_date}")
        print(f"  今日の日付: {TODAY}")
        print(f"  データの古さ: {days_old}日")
    
    return df_old, days_old


def _check_if_update_needed(df_old, days_old, code, latest_trading_date):
    """更新が必要かチェック（既に最新の場合はFalse）"""
    if df_old is None:
        return True
    
    if latest_trading_date and days_old < 2:
        latest_trading_date_str = pd.to_datetime(latest_trading_date).strftime("%Y%m%d")
        last_date = df_old["日期"].max()
        last_date_dt = pd.to_datetime(last_date)
        last_date_str = last_date_dt.strftime("%Y%m%d")
        if last_date_str == latest_trading_date_str:
            if code in ["600838", "603324"]:
                print(f"  → スキップ（既に最新の取引日まで取得済み）")
            return False
    return True


def _determine_start_date(df_old, days_old, code):
    """開始日の決定"""
    if df_old is None:
        start_date = DEFAULT_START
        if code in ["600838", "603324"]:
            print(f"\n[デバッグ開始] {code}: 新規データ取得")
            print(f"  start_date: {start_date}")
        return start_date
    
    last_date = df_old["日期"].max()
    last_date_dt = pd.to_datetime(last_date)
    
    if days_old >= 2:
        start_date = (last_date_dt - pd.Timedelta(days=3)).strftime("%Y%m%d")
        if code in ["600838", "603324"]:
            print(f"  既存データが古いため、データ取得を試行します")
            print(f"  start_date: {start_date}")
    else:
        start_date = (last_date_dt + pd.Timedelta(days=1)).strftime("%Y%m%d")
        if code in ["600838", "603324"]:
            print(f"  通常のデータ更新を試行します")
            print(f"  start_date: {start_date}")
    
    return start_date


def _fetch_stock_data(code, start_date_str, end_date_str):
    """akshareからデータを取得（1回の試行）"""
    df_new = ak.stock_zh_a_hist(
        symbol=code,
        period="daily",
        start_date=start_date_str,
        end_date=end_date_str,
        adjust="qfq"
    )
    return _normalize_date_column(df_new) if df_new is not None and not df_new.empty else None


def _try_alternative_fetch_when_empty(code, df_old, days_old, end_date_str, path, latest_trading_date):
    """空データ時の代替取得方法"""
    if df_old is None or days_old < 2:
        return None
    
    if code in ["600838", "603324"]:
        print(f"[デバッグ] {code}: データ取得試行したが空データ。代替方法で取得を試行")
    
    try:
        if latest_trading_date:
            alt_start_date = (pd.to_datetime(latest_trading_date) - pd.Timedelta(days=5)).strftime("%Y%m%d")
        else:
            alt_start_date = (pd.to_datetime(TODAY) - pd.Timedelta(days=5)).strftime("%Y%m%d")
        
        if code in ["600838", "603324"]:
            print(f"[デバッグ] {code}: 代替方法で取得を試行。start_date={alt_start_date}, end_date={end_date_str}")
        
        df_alt = _fetch_stock_data(code, alt_start_date, end_date_str)
        if df_alt is not None and not df_alt.empty:
            df_old["日期"] = df_old["日期"].astype(str)
            df_alt["日期"] = df_alt["日期"].astype(str)
            df = pd.concat([df_old, df_alt], ignore_index=True)
            df = df.drop_duplicates(subset=["日期"]).sort_values("日期")
            df.to_csv(path, index=False, encoding="utf-8-sig")
            if code in ["600838", "603324"]:
                print(f"[デバッグ] {code}: 代替方法で取得成功")
            return df
    except Exception as e_alt:
        if code in ["600838", "603324"]:
            print(f"[デバッグ] {code}: 代替方法でも失敗: {str(e_alt)}")
    return None


def _merge_and_save_data(df_old, df_new, path, code):
    """データ結合と保存"""
    try:
        if df_old is not None:
            df_old["日期"] = df_old["日期"].astype(str).str.replace("/", "-")
            df_new["日期"] = df_new["日期"].astype(str).str.replace("/", "-")
            df = pd.concat([df_old, df_new], ignore_index=True)
        else:
            df = df_new.copy()
            df["日期"] = df["日期"].astype(str).str.replace("/", "-")
        
        df = df.drop_duplicates(subset=["日期"])
        df["日期_temp"] = pd.to_datetime(df["日期"])
        df = df.sort_values("日期_temp")
        df = df.drop("日期_temp", axis=1)
        
        if code in ["600838", "603324"]:
            print(f"  → データ結合完了。最新日: {df['日期'].max()}")
            print(f"  → CSVに保存中...")
        
        df.to_csv(path, index=False, encoding="utf-8-sig")
        
        if code in ["600838", "603324"]:
            print(f"  → 成功！最新日: {df['日期'].max()}")
        
        return df
    except Exception as e_process:
        if code in ["600838", "603324"]:
            print(f"  → データ結合エラー: {str(e_process)}")
            print(f"  → 取得したデータだけでも保存を試みます...")
        
        try:
            df_new["日期"] = df_new["日期"].astype(str).str.replace("/", "-")
            df_new["日期_temp"] = pd.to_datetime(df_new["日期"])
            df_new = df_new.sort_values("日期_temp")
            df_new = df_new.drop("日期_temp", axis=1)
            
            if df_old is not None:
                df_new.to_csv(f"{path}.new", index=False, encoding="utf-8-sig")
                if code in ["600838", "603324"]:
                    print(f"  → 新しいデータを一時ファイルに保存しました")
            else:
                df_new.to_csv(path, index=False, encoding="utf-8-sig")
                if code in ["600838", "603324"]:
                    print(f"  → 新しいデータを保存しました")
                return df_new
        except Exception as e_save:
            if code in ["600838", "603324"]:
                print(f"  → データ保存も失敗: {str(e_save)}")
        
        if df_old is not None:
            return df_old
        else:
            raise e_process


def _handle_date_type_error(code, df_old, end_date_str, path, latest_trading_date):
    """日付型エラーの処理（方法1と方法2を試行）"""
    if df_old is None:
        return None
    
    # 方法1: 最新の取引日から7日分のデータを取得
    try:
        if latest_trading_date:
            alt_start = (pd.to_datetime(latest_trading_date) - pd.Timedelta(days=7)).strftime("%Y%m%d")
        else:
            alt_start = (pd.to_datetime(TODAY) - pd.Timedelta(days=7)).strftime("%Y%m%d")
        
        if code in ["600838", "603324"]:
            print(f"[デバッグ] {code}: 日付比較エラー。方法1を試行: {alt_start}～{end_date_str}")
        
        df_alt = _fetch_stock_data(code, alt_start, end_date_str)
        if df_alt is not None and not df_alt.empty:
            df_old["日期"] = df_old["日期"].astype(str)
            df_alt["日期"] = df_alt["日期"].astype(str)
            df = pd.concat([df_old, df_alt], ignore_index=True)
            df = df.drop_duplicates(subset=["日期"]).sort_values("日期")
            df.to_csv(path, index=False, encoding="utf-8-sig")
            if code in ["600838", "603324"]:
                print(f"[デバッグ] {code}: 方法1で取得成功")
            return df
    except Exception as e_alt1:
        if code in ["600838", "603324"]:
            print(f"[デバッグ] {code}: 方法1も失敗: {str(e_alt1)}")
    
    # 方法2: 既存データの最新日の次の日から取得
    try:
        last_date_str = str(df_old["日期"].max()).replace("-", "").replace("/", "")
        if len(last_date_str) == 10:
            last_date_str = last_date_str.replace("-", "")
        elif len(last_date_str) != 8:
            last_date_dt = pd.to_datetime(df_old["日期"].max())
            last_date_str = last_date_dt.strftime("%Y%m%d")
        
        year = int(last_date_str[:4])
        month = int(last_date_str[4:6])
        day = int(last_date_str[6:8])
        next_date = pd.Timestamp(year, month, day) + pd.Timedelta(days=1)
        next_date_str = next_date.strftime("%Y%m%d")
        
        if code in ["600838", "603324"]:
            print(f"[デバッグ] {code}: 方法2を試行: {next_date_str}～{end_date_str}")
        
        df_alt2 = _fetch_stock_data(code, next_date_str, end_date_str)
        if df_alt2 is not None and not df_alt2.empty:
            df_old["日期"] = df_old["日期"].astype(str)
            df_alt2["日期"] = df_alt2["日期"].astype(str)
            df = pd.concat([df_old, df_alt2], ignore_index=True)
            df = df.drop_duplicates(subset=["日期"]).sort_values("日期")
            df.to_csv(path, index=False, encoding="utf-8-sig")
            if code in ["600838", "603324"]:
                print(f"[デバッグ] {code}: 方法2で取得成功")
            return df
    except Exception as e_alt2:
        if code in ["600838", "603324"]:
            print(f"[デバッグ] {code}: 方法2も失敗: {str(e_alt2)}")
    
    return None


def load_or_download(code: str, latest_trading_date=None) -> pd.DataFrame:
    """
    既存CSVがあれば不足分だけDLして追記
    ネットワークエラー時は既存データを返す
    既存データが古い（2日以上前）場合は、より広い期間で取得を試みる
    """
    path = f"{DATA_DIR}/{code}.csv"

    # 既存データの読み込み
    df_old, days_old = _load_existing_data(code, path)
    
    # 更新が必要かチェック
    if not _check_if_update_needed(df_old, days_old, code, latest_trading_date):
        return df_old
    
    # 開始日の決定
    start_date = _determine_start_date(df_old, days_old, code)

    if start_date > TODAY:
        if code in ["600838", "603324"]:
            print(f"[デバッグ] {code}: スキップ（start_date={start_date} > TODAY={TODAY}）")
        return df_old

    # データ取得の試行（リトライ付き）
    start_date_str = str(start_date)
    end_date_str = str(TODAY)
    max_retries = 3
    retry_delay = 1
    
    if code in ["600838", "603324"]:
        print(f"  データ取得を試行: start_date={start_date_str}, end_date={end_date_str}")
    
    last_error = None
    for attempt in range(max_retries):
        try:
            if code in ["600838", "603324"]:
                print(f"  試行 {attempt + 1}/{max_retries}: akshare API呼び出し中...")
            
            df_new = _fetch_stock_data(code, start_date_str, end_date_str)
            
            if df_new is not None and not df_new.empty:
                if code in ["600838", "603324"]:
                    print(f"  → 結果: {len(df_new)}件のデータを取得")
                    print(f"  取得期間: {df_new['日期'].min()} ～ {df_new['日期'].max()}")
                
                # データ結合と保存
                return _merge_and_save_data(df_old, df_new, path, code)
            
            # データが空の場合、代替方法を試行
            if df_new is None or df_new.empty:
                df_alt = _try_alternative_fetch_when_empty(code, df_old, days_old, end_date_str, path, latest_trading_date)
                if df_alt is not None:
                    return df_alt
        
        except Exception as e:
            last_error = str(e)
            if code in ["600838", "603324"]:
                print(f"  → エラー発生: {str(e)}")
            
            # 日付比較エラーの場合、別のアプローチを試す
            if "'<' not supported between instances of 'datetime.date' and 'str'" in str(e) or "not supported between instances" in str(e):
                df_alt = _handle_date_type_error(code, df_old, end_date_str, path, latest_trading_date)
                if df_alt is not None:
                    return df_alt
            
            # リトライ
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
    
    # 全試行失敗
    if code in ["600838", "603324"]:
        print(f"  → 全{max_retries}回の試行が失敗しました")
        print(f"  最後のエラー: {last_error}")

    if df_old is not None:
        print(f"  → 既存データを返します（最新日: {df_old['日期'].max()}）")
        return df_old
    else:
        # 既存データがない場合、Noneを返して失敗として記録
        if code in ["600838", "603324"]:
            print(f"  → 既存データがないため、Noneを返します（エラー: {last_error}）")
        return None


def get_latest_trading_date(stocks):
    """
    サンプル銘柄で最新の取引日を確認
    """
    import akshare as ak
    from datetime import datetime, timedelta
    from config import TODAY
    
    if len(stocks) == 0:
        print("[警告] 銘柄リストが空のため、最新取引日を取得できません")
        return None
    
    sample_code = str(stocks.iloc[0]["代码"])  # 文字列型に変換
    max_retries = 3
    for attempt in range(max_retries):
        try:
            df = ak.stock_zh_a_hist(
                symbol=sample_code,
                period="daily",
                start_date=(datetime.today() - timedelta(days=10)).strftime("%Y%m%d"),
                end_date=TODAY,
                adjust="qfq"
            )
            if df is not None and not df.empty:
                latest = df.sort_values("日期").iloc[-1]["日期"]
                return latest
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
            else:
                print(f"[警告] 最新取引日の取得に失敗: {str(e)}")
    return None

