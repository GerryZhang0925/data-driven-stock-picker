"""
ファンダメンタル分析関連（日本株）
PER、PBR、時価総額などの情報を取得
"""
import yfinance as yf
import pandas as pd
import os
import time
from tqdm import tqdm
from config import DATA_DIR_JP, TODAY

# ファンダメンタル情報の保存パス
FUNDAMENTALS_PATH_JP = f"{DATA_DIR_JP}/fundamentals_jp.csv"


def get_stock_fundamentals(code):
    """
    銘柄のファンダメンタル情報を取得
    """
    try:
        ticker = f"{code}.T"
        stock = yf.Ticker(ticker)
        info = stock.info
        
        if not info or len(info) == 0:
            return None
        
        # PER、PBR、時価総額を取得
        per = info.get("trailingPE") or info.get("forwardPE")
        pbr = info.get("priceToBook")
        market_cap = info.get("marketCap")
        
        # 営業利益率とROAを取得
        operating_margin = info.get("operatingMargin") or info.get("operatingMargins")
        # operatingMarginsがリストの場合は最新の値を取得
        if isinstance(operating_margin, list) and len(operating_margin) > 0:
            operating_margin = operating_margin[-1]
        
        roa = info.get("returnOnAssets") or info.get("roa")
        # roaがリストの場合は最新の値を取得
        if isinstance(roa, list) and len(roa) > 0:
            roa = roa[-1]
        
        # その他の有用な情報も取得
        # 日本語名を取得（stock_list_jp.csvから優先的に取得）
        name = None
        try:
            from jp.stock_list import STOCK_LIST_PATH_JP
            import os
            if os.path.exists(STOCK_LIST_PATH_JP):
                stock_list_df = pd.read_csv(STOCK_LIST_PATH_JP, encoding="utf-8-sig")
                if "代码" in stock_list_df.columns and "名称" in stock_list_df.columns:
                    stock_list_df["代码"] = stock_list_df["代码"].astype(str)
                    matched = stock_list_df[stock_list_df["代码"] == str(code)]
                    if not matched.empty:
                        name = matched.iloc[0]["名称"]
        except Exception:
            pass
        
        # 日本語名が取得できなかった場合はyfinanceから取得
        if not name:
            name = info.get("longName", info.get("shortName", f"銘柄{code}"))
        
        sector = info.get("sector", "")
        industry = info.get("industry", "")
        dividend_yield = info.get("dividendYield")
        eps = info.get("trailingEps") or info.get("forwardEps")
        
        # ROE（自己資本利益率）も取得可能な場合は追加
        roe = info.get("returnOnEquity")
        if isinstance(roe, list) and len(roe) > 0:
            roe = roe[-1]
        
        return {
            "代码": code,
            "名称": name,
            "PER": per if per is not None else None,
            "PBR": pbr if pbr is not None else None,
            "時価総額": market_cap if market_cap is not None else None,
            "営業利益率": operating_margin * 100 if operating_margin is not None else None,
            "ROA": roa * 100 if roa is not None else None,
            "ROE": roe * 100 if roe is not None else None,
            "セクター": sector,
            "業種": industry,
            "配当利回り": dividend_yield * 100 if dividend_yield is not None else None,
            "EPS": eps if eps is not None else None,
            "更新日": TODAY
        }
    except Exception as e:
        print(f"銘柄 {code} のファンダメンタル情報取得に失敗: {str(e)}")
        return None


def get_all_fundamentals(stocks_df=None):
    """
    全銘柄のファンダメンタル情報を取得
    """
    from jp.stock_list import get_stock_list
    
    if stocks_df is None:
        stocks_df = get_stock_list()
    
    print(f"\n全{len(stocks_df)}銘柄のファンダメンタル情報を取得中...")
    
    fundamentals_list = []
    
    for _, row in tqdm(stocks_df.iterrows(), total=len(stocks_df), desc="ファンダメンタル取得"):
        code = row["代码"]
        fundamentals = get_stock_fundamentals(code)
        
        if fundamentals:
            fundamentals_list.append(fundamentals)
        
        # APIレート制限対策
        time.sleep(0.1)
    
    if not fundamentals_list:
        print("ファンダメンタル情報が取得できませんでした")
        return None
    
    fundamentals_df = pd.DataFrame(fundamentals_list)
    return fundamentals_df


def save_fundamentals(fundamentals_df, path=None):
    """
    ファンダメンタル情報をCSVに保存
    """
    if path is None:
        path = FUNDAMENTALS_PATH_JP
    
    fundamentals_df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"ファンダメンタル情報を保存しました: {path}")
    print(f"  取得銘柄数: {len(fundamentals_df)}銘柄")
    print(f"  PERデータあり: {fundamentals_df['PER'].notna().sum()}銘柄")
    print(f"  PBRデータあり: {fundamentals_df['PBR'].notna().sum()}銘柄")
    print(f"  時価総額データあり: {fundamentals_df['時価総額'].notna().sum()}銘柄")
    if '営業利益率' in fundamentals_df.columns:
        print(f"  営業利益率データあり: {fundamentals_df['営業利益率'].notna().sum()}銘柄")
    if 'ROA' in fundamentals_df.columns:
        print(f"  ROAデータあり: {fundamentals_df['ROA'].notna().sum()}銘柄")
    if 'ROE' in fundamentals_df.columns:
        print(f"  ROEデータあり: {fundamentals_df['ROE'].notna().sum()}銘柄")


def load_fundamentals(path=None):
    """
    保存済みのファンダメンタル情報を読み込み
    """
    if path is None:
        path = FUNDAMENTALS_PATH_JP
    
    if os.path.exists(path):
        try:
            df = pd.read_csv(path, encoding="utf-8-sig")
            print(f"保存済みファンダメンタル情報を読み込み: {len(df)}銘柄")
            return df
        except Exception as e:
            print(f"ファンダメンタル情報の読み込みに失敗: {str(e)}")
            return None
    return None


def update_fundamentals(force_update=False):
    """
    ファンダメンタル情報を更新
    """
    # 保存済みデータを確認
    existing_df = load_fundamentals()
    
    if existing_df is not None and not force_update:
        # 更新日を確認（7日以上古い場合は更新）
        if "更新日" in existing_df.columns:
            latest_update = existing_df["更新日"].max()
            if latest_update:
                # 日付を文字列型に統一して比較
                latest_update_str = str(latest_update)
                today_str = str(TODAY)
                # 日付形式を統一（YYYYMMDD形式に変換）
                if len(latest_update_str) == 10:  # YYYY-MM-DD形式の場合
                    latest_update_str = latest_update_str.replace("-", "")
                if len(today_str) == 10:  # YYYY-MM-DD形式の場合
                    today_str = today_str.replace("-", "")
                
                if latest_update_str >= today_str:
                    print(f"ファンダメンタル情報は最新です（更新日: {latest_update}）")
                    return existing_df
    
    # 最新データを取得
    fundamentals_df = get_all_fundamentals()
    
    if fundamentals_df is not None:
        save_fundamentals(fundamentals_df)
        return fundamentals_df
    
    # 取得失敗時は既存データを返す
    if existing_df is not None:
        print("最新データの取得に失敗したため、既存データを使用します")
        return existing_df
    
    return None
