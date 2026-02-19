"""
日本株の銘柄リスト取得関連
"""
import pandas as pd
import os
import time
from datetime import datetime
from config import DATA_DIR_JP, TODAY

STOCK_LIST_PATH_JP = f"{DATA_DIR_JP}/stock_list_jp.csv"


def get_stock_list(no_download=False):
    """
    日本株の銘柄リストを取得
    保存済みのリストがあれば使用、なければ既存データから構築
    
    Args:
        no_download: Trueの場合、銘柄リストの更新をスキップ
    """
    # 保存済みのリストがあれば使用
    if os.path.exists(STOCK_LIST_PATH_JP):
        try:
            stocks = pd.read_csv(STOCK_LIST_PATH_JP, encoding="utf-8-sig")
            # コード列を文字列型に変換（念のため）
            if "代码" in stocks.columns:
                stocks["代码"] = stocks["代码"].astype(str)
            
            # 更新日時を確認
            file_time = os.path.getmtime(STOCK_LIST_PATH_JP)
            file_datetime = datetime.fromtimestamp(file_time)
            days_old = (datetime.now() - file_datetime).days
            
            print(f"保存済み銘柄リストを読み込み: {len(stocks)}銘柄（更新日時: {file_datetime.strftime('%Y-%m-%d %H:%M:%S')}）")
            
            # 7日以上古い場合は更新を試行（no_download時はスキップ）
            if days_old >= 7 and not no_download:
                print("銘柄リストが7日以上古いため、更新を試行します...")
                # 既存データから最新のリストを構築
                new_stocks = _build_stock_list_from_data()
                if new_stocks is not None and not new_stocks.empty:
                    new_stocks.to_csv(STOCK_LIST_PATH_JP, index=False, encoding="utf-8-sig")
                    print(f"銘柄リストを更新しました: {len(new_stocks)}銘柄")
                    return new_stocks
            elif not no_download:
                # 7日以内でもバックグラウンドで更新を試行（no_download時はスキップ）
                new_stocks = _build_stock_list_from_data()
                if new_stocks is not None and not new_stocks.empty and len(new_stocks) > len(stocks):
                    new_stocks.to_csv(STOCK_LIST_PATH_JP, index=False, encoding="utf-8-sig")
                    print(f"銘柄リストを更新しました: {len(new_stocks)}銘柄")
                    return new_stocks
            
            return stocks
        except Exception as e:
            print(f"保存済み銘柄リストの読み込みに失敗: {str(e)}")
    
    # 保存済みリストがない場合、またはno_downloadでない場合は構築
    if not no_download:
        print("日本株の全銘柄リストを取得中...")
        stocks = _build_stock_list_from_data()
        
        if stocks is not None and not stocks.empty:
            stocks.to_csv(STOCK_LIST_PATH_JP, index=False, encoding="utf-8-sig")
            print(f"銘柄リストを更新しました: {len(stocks)}銘柄")
            return stocks
    
    # 既存データからも取得できない場合は空のDataFrameを返す
    print("警告: 銘柄リストを取得できませんでした。")
    return pd.DataFrame(columns=["代码", "名称"])


def _build_stock_list_from_data():
    """
    既存のCSVファイルから銘柄リストを構築
    """
    if not os.path.exists(DATA_DIR_JP):
        return None
    
    csv_files = [f for f in os.listdir(DATA_DIR_JP) if f.endswith('.csv') and not f.startswith('stock_list')]
    
    if not csv_files:
        return None
    
    codes = []
    names = []
    
    # 既存のCSVファイルから銘柄コードを取得
    for csv_file in csv_files:
        code = csv_file.replace('.csv', '')
        # 4桁の数字コードのみを対象
        if code.isdigit() and len(code) == 4:
            codes.append(code)
            # 名称は後で取得（yfinanceから）
            names.append(None)
    
    if not codes:
        return None
    
    print(f"既存データから {len(codes)}銘柄を発見しました")
    
    # 名称を取得（yfinanceから）
    import yfinance as yf
    from tqdm import tqdm
    
    print("名称取得: ", end="")
    for i, code in enumerate(tqdm(codes, desc="名称取得")):
        try:
            # 日本語名を取得（Yahoo!ファイナンスの日本語サイトから）
            name = None
            try:
                import requests
                from bs4 import BeautifulSoup
                
                url = f"https://finance.yahoo.co.jp/quote/{code}.T"
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }
                
                response = requests.get(url, headers=headers, timeout=5)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    # タイトルから会社名を抽出
                    title = soup.find('title')
                    if title:
                        title_text = title.get_text()
                        # "銘柄コード - 会社名 - Yahoo!ファイナンス" の形式から会社名を抽出
                        if ' - ' in title_text:
                            parts = title_text.split(' - ')
                            if len(parts) >= 2:
                                name = parts[1].strip()
                                if name and name != '' and name != 'Yahoo!ファイナンス':
                                    names[i] = name
                                    time.sleep(0.1)  # APIレート制限対策
                                    continue
            except ImportError:
                # beautifulsoup4やrequestsがインストールされていない場合
                pass
            except Exception:
                # スクレイピングに失敗した場合
                pass
            
            # スクレイピングに失敗した場合はyfinanceから取得
            if not name:
                ticker = f"{code}.T"
                stock = yf.Ticker(ticker)
                info = stock.info
                if info and len(info) > 0:
                    name = info.get("longName") or info.get("shortName") or f"銘柄{code}"
                    names[i] = name
                else:
                    names[i] = f"銘柄{code}"
            time.sleep(0.1)  # APIレート制限対策
        except Exception:
            names[i] = f"銘柄{code}"
    
    # DataFrameを作成
    stocks = pd.DataFrame({
        "代码": codes,
        "名称": names
    })
    
    print(f"既存データから {len(stocks)}銘柄を取得しました")
    return stocks
