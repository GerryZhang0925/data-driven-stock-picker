"""
日本株のデータ取得・ダウンロード関連
"""
import yfinance as yf
import pandas as pd
import os
import time
from datetime import datetime, timedelta
from config import DATA_DIR_JP, TODAY, DEFAULT_START


def _load_existing_data(path):
    """
    既存データを読み込み、日付を正規化
    """
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
        if "日期" in df.columns:
            # 日付列を正規化
            df["日期"] = pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d")
        return df
    except Exception as e:
        return None


def _check_if_update_needed(df_old, days_old, code, latest_trading_date):
    """
    データ更新が必要かどうかをチェック
    """
    if df_old is None or df_old.empty:
        return True
    
    if latest_trading_date is None:
        return True
    
    # 最新取引日と既存データの最新日を比較
    latest_date_str = pd.to_datetime(latest_trading_date).strftime("%Y-%m-%d")
    old_latest_date_str = pd.to_datetime(df_old["日期"].max()).strftime("%Y-%m-%d")
    
    if old_latest_date_str >= latest_date_str:
        return False
    
    # 2日以上古い場合は更新
    if days_old >= 2:
        return True
    
    return False


def _convert_yfinance_to_standard_format(df_yf):
    """
    yfinanceのデータを標準形式（中国株と同じ形式）に変換
    """
    if df_yf is None or df_yf.empty:
        return None
    
    # 列名を中国株と同じ形式に変換
    df = df_yf.copy()
    df = df.reset_index()
    
    # 列名のマッピング
    column_mapping = {
        'Date': '日期',
        'Open': '开盘',
        'High': '最高',
        'Low': '最低',
        'Close': '收盘',
        'Volume': '成交量',
    }
    
    # 列名を変更
    df = df.rename(columns=column_mapping)
    
    # 日付列を文字列形式に変換
    if '日期' in df.columns:
        df['日期'] = pd.to_datetime(df['日期']).dt.strftime("%Y-%m-%d")
    
    # 漲跌幅（前日比）を計算
    if '收盘' in df.columns:
        df['涨跌幅'] = df['收盘'].pct_change() * 100
        df['涨跌幅'] = df['涨跌幅'].fillna(0)
    
    # 成交额（取引額）を計算（Volume * Close）
    if '成交量' in df.columns and '收盘' in df.columns:
        df['成交额'] = df['成交量'] * df['收盘']
    
    # 必要な列のみを選択
    required_columns = ['日期', '开盘', '最高', '最低', '收盘', '成交量', '涨跌幅', '成交额']
    available_columns = [col for col in required_columns if col in df.columns]
    df = df[available_columns]
    
    return df


def load_or_download(code: str, latest_trading_date=None, force_update=False) -> pd.DataFrame:
    """
    既存CSVがあれば不足分だけDLして追記
    既存データが古い（2日以上前）場合は、より広い期間で取得を試みる
    
    Args:
        code: 銘柄コード（4桁の数字、例: "7203"）
        latest_trading_date: 最新取引日（文字列形式: "YYYY-MM-DD"）
        force_update: 強制更新フラグ
    """
    path = f"{DATA_DIR_JP}/{code}.csv"
    
    # 既存データを読み込み
    df_old = None
    days_old = None
    if os.path.exists(path) and not force_update:
        df_old = _load_existing_data(path)
        if df_old is not None and not df_old.empty:
            # 既存データの最新日を確認
            old_latest_date = pd.to_datetime(df_old["日期"].max())
            if latest_trading_date:
                latest_trading_date_dt = pd.to_datetime(latest_trading_date)
                days_old = (latest_trading_date_dt - old_latest_date).days
            else:
                days_old = (pd.to_datetime(TODAY, format="%Y%m%d") - old_latest_date).days
    
    # 強制更新の場合は、既存データがあっても最新データを取得
    if force_update:
        df_old = None
        days_old = None
    
    # 更新が必要かチェック
    if not force_update and df_old is not None:
        if not _check_if_update_needed(df_old, days_old or 0, code, latest_trading_date):
            return df_old
    
    # データ取得の開始日を決定
    if df_old is not None and not force_update:
        last_date = df_old["日期"].max()
        start_date = (pd.to_datetime(last_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        # デフォルト開始日をYYYY-MM-DD形式に変換
        start_date = pd.to_datetime(DEFAULT_START, format="%Y%m%d").strftime("%Y-%m-%d")
    
    # 終了日を決定
    end_date = pd.to_datetime(TODAY, format="%Y%m%d").strftime("%Y-%m-%d")
    
    if pd.to_datetime(start_date) > pd.to_datetime(end_date):
        return df_old
    
    # データ取得を試行
    max_retries = 3
    for attempt in range(max_retries):
        try:
            ticker = f"{code}.T"
            stock = yf.Ticker(ticker)
            
            # yfinanceでデータを取得
            df_new = stock.history(start=start_date, end=end_date)
            
            if df_new is None or df_new.empty:
                # データが取得できなかった場合は既存データを返す
                return df_old
            
            # 標準形式に変換
            df_new = _convert_yfinance_to_standard_format(df_new)
            
            if df_new is None or df_new.empty:
                return df_old
            
            # 既存データと結合
            if df_old is not None:
                df = pd.concat([df_old, df_new], ignore_index=True)
                df = df.drop_duplicates(subset=["日期"]).sort_values("日期")
            else:
                df = df_new.sort_values("日期")
            
            # 保存
            df.to_csv(path, index=False, encoding="utf-8-sig")
            return df
            
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(1)  # リトライ前に少し待機
                continue
            else:
                # 最終的に失敗した場合は既存データを返す
                return df_old
    
    return df_old


def get_latest_trading_date(stocks, sample_size=5):
    """
    サンプル銘柄で最新の取引日を確認
    
    Args:
        stocks: 銘柄リスト（DataFrame）
        sample_size: サンプル銘柄数
    """
    if stocks is None or stocks.empty:
        return None
    
    # サンプル銘柄を選択（先頭から）
    sample_stocks = stocks.head(sample_size)
    
    latest_dates = []
    for _, row in sample_stocks.iterrows():
        code = row["代码"]
        try:
            ticker = f"{code}.T"
            stock = yf.Ticker(ticker)
            
            # 最新のデータを取得（10日分）
            end_date = datetime.now()
            start_date = end_date - timedelta(days=10)
            df = stock.history(start=start_date.strftime("%Y-%m-%d"), end=end_date.strftime("%Y-%m-%d"))
            
            if df is not None and not df.empty:
                latest_date = df.index.max()
                latest_dates.append(latest_date)
            
            time.sleep(0.1)  # APIレート制限対策
            
        except Exception:
            continue
    
    if latest_dates:
        # 最新の日付を返す
        latest = max(latest_dates)
        return pd.to_datetime(latest).strftime("%Y-%m-%d")
    
    return None
