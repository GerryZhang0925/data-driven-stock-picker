"""
銘柄リスト取得関連
"""
import akshare as ak
import pandas as pd
import os
import time
from datetime import datetime, timedelta
from config import STOCK_LIST_PATH, DATA_DIR, TODAY


def get_xsb_stocks_only():
    """
    新三板（北交所含む）のみの銘柄リストを取得
    """
    print("新三板の銘柄リストを取得中...")
    
    # 方法1: 新三板専用APIを試す
    xsb_apis = [
        'stock_xsb_list_em',
        'stock_xsb_spot_em',
        'stock_xsb_info_em',
        'stock_xsb_stock_list',
    ]
    
    for api_name in xsb_apis:
        if hasattr(ak, api_name):
            try:
                print(f"新三板APIを試行: {api_name}")
                func = getattr(ak, api_name)
                stocks_xsb = func()
                if stocks_xsb is not None and not stocks_xsb.empty:
                    print(f"新三板API取得成功: {api_name}, {len(stocks_xsb)}銘柄")
                    # 列名を確認して統一
                    if "代码" not in stocks_xsb.columns:
                        if "证券代码" in stocks_xsb.columns:
                            stocks_xsb = stocks_xsb.rename(columns={"证券代码": "代码", "证券简称": "名称"})
                        elif "code" in stocks_xsb.columns:
                            stocks_xsb = stocks_xsb.rename(columns={"code": "代码", "name": "名称"})
                        elif "股票代码" in stocks_xsb.columns:
                            stocks_xsb = stocks_xsb.rename(columns={"股票代码": "代码", "股票简称": "名称"})
                    
                    if "代码" in stocks_xsb.columns:
                        stocks_xsb["代码"] = stocks_xsb["代码"].astype(str)
                        # 新三板のコードは43、83、87で始まる
                        stocks_xsb = stocks_xsb[
                            stocks_xsb["代码"].str.startswith('43') | 
                            stocks_xsb["代码"].str.startswith('83') | 
                            stocks_xsb["代码"].str.startswith('87')
                        ]
                        if not stocks_xsb.empty:
                            print(f"新三板フィルタリング後: {len(stocks_xsb)}銘柄")
                            print(f"新三板コードのサンプル: {stocks_xsb['代码'].head(10).tolist()}")
                            return stocks_xsb
            except Exception as e:
                print(f"新三板API {api_name} の取得に失敗: {str(e)}")
                continue
    
    # 方法2: 北交所（北京交易所）の銘柄を試す（新三板から移行した銘柄がある可能性）
    try:
        print("北交所の銘柄リストを取得中...")
        if hasattr(ak, 'stock_bj_a_spot_em'):
            stocks_bj = ak.stock_bj_a_spot_em()
            if stocks_bj is not None and not stocks_bj.empty:
                if "代码" in stocks_bj.columns:
                    stocks_bj["代码"] = stocks_bj["代码"].astype(str)
                    # デバッグ: 取得した銘柄のコードの先頭文字を確認
                    code_prefixes = stocks_bj["代码"].str[:1].value_counts().head(10)
                    print(f"[デバッグ] stock_bj_a_spot_em()が返した銘柄の先頭1文字: {dict(code_prefixes)}")
                    # 8または9で始まる銘柄を返す（北交所の銘柄）
                    stocks_bj = stocks_bj[
                        stocks_bj["代码"].str.startswith('8') | 
                        stocks_bj["代码"].str.startswith('9')
                    ]
                    print(f"北交所銘柄（8または9で始まる）: {len(stocks_bj)}銘柄")
                    return stocks_bj
    except Exception as e:
        print(f"北交所の取得に失敗: {str(e)}")
    
    # 方法3: 新三板のサンプルコードでデータ取得を試す（データ取得が可能か確認）
    print("新三板のデータ取得を試行中（サンプルコードで確認）...")
    sample_xsb_codes = ['430001', '430002', '830001', '830002', '870001', '870002']  # 新三板のサンプルコード
    valid_xsb_codes = []
    
    for code in sample_xsb_codes[:3]:  # 最初の3つだけ試す
        try:
            df_test = ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=(datetime.today() - timedelta(days=5)).strftime("%Y%m%d"),
                end_date=TODAY,
                adjust="qfq"
            )
            if df_test is not None and not df_test.empty:
                valid_xsb_codes.append(code)
                print(f"新三板コード {code} のデータ取得に成功")
        except Exception as e:
            print(f"新三板コード {code} のデータ取得に失敗: {str(e)}")
            continue
    
    if valid_xsb_codes:
        print(f"新三板のデータ取得が可能であることを確認しました（サンプル: {valid_xsb_codes}）")
        print("注意: 新三板の銘柄リストは手動で作成するか、別のデータソースから取得する必要があります")
        print("新三板のコードは43、83、87で始まる8桁の数字です")
    
    print("警告: 新三板の銘柄リストを取得できませんでした")
    print("新三板をサポートするには、新三板の銘柄リストを手動でCSVファイルに追加する必要があります")
    print("新三板のコード形式: 43xxxxxx（基礎層）、83xxxxxx（創新層）、87xxxxxx（精選層）")
    return None


def get_stocks_from_api():
    """
    APIから上海A株と新三板（北交所含む）の銘柄リストを取得
    """
    all_stocks = []
    errors = []
    
    # 上海A株（60で始まる）と科創板（68で始まる）
    max_retries_sh = 3
    for attempt in range(max_retries_sh):
        try:
            stocks_sh = ak.stock_sh_a_spot_em()
            stocks_sh["代码"] = stocks_sh["代码"].astype(str)
            
            # デバッグ: 取得した全銘柄のコードの先頭文字を確認
            if attempt == 0:
                code_prefixes = stocks_sh["代码"].str[:2].value_counts().head(10)
                print(f"[デバッグ] stock_sh_a_spot_em()が返した銘柄の先頭2文字: {dict(code_prefixes)}")
            
            # 60（上海A株）と68（科創板）で始まる銘柄を含める
            stocks_sh = stocks_sh[
                stocks_sh["代码"].str.startswith("60") | 
                stocks_sh["代码"].str.startswith("68")
            ]
            all_stocks.append(stocks_sh)
            count_60 = stocks_sh[stocks_sh["代码"].str.startswith("60")].shape[0]
            count_68 = stocks_sh[stocks_sh["代码"].str.startswith("68")].shape[0]
            print(f"上海A株（60）: {count_60}銘柄, 科創板（68）: {count_68}銘柄, 合計: {len(stocks_sh)}銘柄")
            break
        except Exception as e:
            error_msg = f"上海A株の取得に失敗 (試行 {attempt + 1}/{max_retries_sh}): {str(e)}"
            if attempt < max_retries_sh - 1:
                print(error_msg)
                time.sleep(2)
            else:
                print(error_msg)
                errors.append("上海A株の取得に失敗")
    
    # 新三板（43、83、87で始まる）
    try:
        stocks_xsb = get_xsb_stocks_only()
        if stocks_xsb is not None and not stocks_xsb.empty:
            all_stocks.append(stocks_xsb)
            print(f"新三板: {len(stocks_xsb)}銘柄")
        else:
            errors.append("新三板の取得に失敗（データが空）")
    except Exception as e:
        print(f"新三板の取得に失敗: {str(e)}")
        errors.append(f"新三板の取得に失敗: {str(e)}")
    
    if not all_stocks:
        error_summary = "; ".join(errors) if errors else "不明なエラー"
        raise Exception(f"銘柄リストが取得できませんでした: {error_summary}")
    
    # すべての銘柄を結合
    stocks_combined = pd.concat(all_stocks, ignore_index=True)
    
    # デバッグ: 結合前の全銘柄のコードの先頭文字を確認
    code_prefixes = stocks_combined["代码"].str[:2].value_counts().head(10)
    print(f"[デバッグ] 結合前の全銘柄の先頭2文字: {dict(code_prefixes)}")
    
    # 重複除去
    before_dedup = len(stocks_combined)
    stocks_combined = stocks_combined.drop_duplicates(subset=["代码"])
    after_dedup = len(stocks_combined)
    if before_dedup != after_dedup:
        print(f"[デバッグ] 重複除去: {before_dedup} → {after_dedup}銘柄（{before_dedup - after_dedup}件の重複を削除）")
    
    # 60、68、43、83、87、8、9で始まらない銘柄をフィルタリング
    valid_mask = (stocks_combined["代码"].str.startswith('60') | 
                  stocks_combined["代码"].str.startswith('68') |
                  stocks_combined["代码"].str.startswith('43') | 
                  stocks_combined["代码"].str.startswith('83') | 
                  stocks_combined["代码"].str.startswith('87') | 
                  stocks_combined["代码"].str.startswith('8') |
                  stocks_combined["代码"].str.startswith('9'))
    invalid_count = (~valid_mask).sum()
    if invalid_count > 0:
        invalid_codes = stocks_combined[~valid_mask]["代码"].head(20).tolist()
        print(f"[警告] 分類外の銘柄が{invalid_count}件あります（例: {', '.join(invalid_codes[:10])}）")
        stocks_combined = stocks_combined[valid_mask]
        print(f"[デバッグ] フィルタリング後: {len(stocks_combined)}銘柄")
    
    return stocks_combined


def get_stock_list():
    """
    銘柄リストを取得（上海A株＋新三板（北交所含む））。保存済みのリストがあれば使用、なければ取得して保存
    """
    # 保存済みのリストがあれば使用
    if os.path.exists(STOCK_LIST_PATH):
        try:
            stocks = pd.read_csv(STOCK_LIST_PATH, encoding="utf-8-sig")
            # コード列を文字列型に変換（念のため）
            if "代码" in stocks.columns:
                stocks["代码"] = stocks["代码"].astype(str)
            print(f"保存済み銘柄リストを読み込み: {len(stocks)}銘柄")
            # 新三板が含まれているか確認（43、83、87、8、9で始まるコード = 新三板＋北交所）
            xsb_mask = (stocks["代码"].str.startswith('43') | 
                       stocks["代码"].str.startswith('83') | 
                       stocks["代码"].str.startswith('87') | 
                       stocks["代码"].str.startswith('8') |
                       stocks["代码"].str.startswith('9'))
            xsb_count = xsb_mask.sum()
            # 60（上海A株）と68（科創板）で始まる銘柄を含める
            sh_mask = stocks["代码"].str.startswith('60') | stocks["代码"].str.startswith('68')
            sh_count = sh_mask.sum()
            
            # デバッグ: 各プレフィックスの数を確認
            count_43 = stocks["代码"].str.startswith('43').sum()
            count_83 = stocks["代码"].str.startswith('83').sum()
            count_87 = stocks["代码"].str.startswith('87').sum()
            count_8 = stocks["代码"].str.startswith('8').sum()
            count_9 = stocks["代码"].str.startswith('9').sum()
            count_60 = stocks["代码"].str.startswith('60').sum()
            count_68 = stocks["代码"].str.startswith('68').sum()
            
            # 分類されていない銘柄を確認
            other_mask = ~(sh_mask | xsb_mask)
            other_count = other_mask.sum()
            if other_count > 0:
                other_codes = stocks[other_mask]["代码"].head(20).tolist()
                print(f"  内訳: 上海A株 {sh_count}銘柄, 新三板（北交所含む） {xsb_count}銘柄, その他 {other_count}銘柄")
                print(f"  注意: 分類外の銘柄が{other_count}件あります（例: {', '.join(other_codes[:10])}）")
            else:
                print(f"  内訳: 上海A株 {sh_count}銘柄, 新三板（北交所含む） {xsb_count}銘柄")
            
            # デバッグ情報を表示（新三板の内訳と上海A株の内訳）
            if xsb_count > 0:
                print(f"  [デバッグ] 新三板内訳: 43={count_43}, 83={count_83}, 87={count_87}, 8={count_8}, 9={count_9}")
            if sh_count > 0:
                print(f"  [デバッグ] 上海A株内訳: 60={count_60}, 68={count_68}")
            
            # 重複チェック
            duplicate_count = stocks["代码"].duplicated().sum()
            if duplicate_count > 0:
                print(f"  警告: 重複コードが{duplicate_count}件あります")
            
            # バックグラウンドで最新リストを取得して更新（エラー時は無視）
            try:
                stocks_new = get_stocks_from_api()
                stocks_new.to_csv(STOCK_LIST_PATH, index=False, encoding="utf-8-sig")
                xsb_mask_new = (stocks_new["代码"].str.startswith('43') | 
                                stocks_new["代码"].str.startswith('83') | 
                                stocks_new["代码"].str.startswith('87') | 
                                stocks_new["代码"].str.startswith('8') |
                                stocks_new["代码"].str.startswith('9'))
                xsb_count_new = xsb_mask_new.sum()
                sh_mask_new = stocks_new["代码"].str.startswith('60') | stocks_new["代码"].str.startswith('68')
                sh_count_new = sh_mask_new.sum()
                count_60_new = stocks_new["代码"].str.startswith('60').sum()
                count_68_new = stocks_new["代码"].str.startswith('68').sum()
                print(f"銘柄リストを更新しました: {len(stocks_new)}銘柄 (上海A株（60）: {count_60_new}, 科創板（68）: {count_68_new}, 合計: {sh_count_new}銘柄, 新三板（北交所含む） {xsb_count_new}銘柄)")
                return stocks_new
            except Exception as e:
                # 更新失敗時は保存済みリストを使用
                print(f"銘柄リスト更新失敗（保存済みリストを使用）: {str(e)}")
                # 新三板が含まれていない場合は追加を試みる
                if xsb_count == 0:
                    print("新三板が含まれていないため、追加を試みます...")
                    try:
                        stocks_xsb = get_xsb_stocks_only()
                        if stocks_xsb is not None and not stocks_xsb.empty:
                            stocks = pd.concat([stocks, stocks_xsb], ignore_index=True)
                            stocks = stocks.drop_duplicates(subset=["代码"])
                            stocks.to_csv(STOCK_LIST_PATH, index=False, encoding="utf-8-sig")
                            print(f"新三板を追加しました: {len(stocks_xsb)}銘柄 (合計: {len(stocks)}銘柄)")
                    except Exception as e_xsb:
                        print(f"新三板の追加に失敗: {str(e_xsb)}")
                return stocks
        except Exception as e:
            print(f"保存済みリストの読み込み失敗: {str(e)}")
    
    # 保存済みリストがない場合は取得
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            stocks = get_stocks_from_api()
            # 取得成功したら保存
            stocks.to_csv(STOCK_LIST_PATH, index=False, encoding="utf-8-sig")
            print(f"銘柄リスト取得成功: {len(stocks)}銘柄")
            return stocks
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"銘柄リスト取得失敗 (試行 {attempt + 1}/{max_retries}): {str(e)}")
                print(f"{retry_delay}秒後に再試行します...")
                time.sleep(retry_delay)
            else:
                print(f"銘柄リスト取得失敗 (全試行終了): {str(e)}")
                # 保存済みリストがあれば使用
                if os.path.exists(STOCK_LIST_PATH):
                    try:
                        stocks = pd.read_csv(STOCK_LIST_PATH, encoding="utf-8-sig")
                        print(f"保存済み銘柄リストを使用: {len(stocks)}銘柄")
                        return stocks
                    except Exception:
                        pass
                
                print("既存データから銘柄リストを取得します...")
                # 既存のCSVファイルから銘柄コードを取得
                if os.path.exists(DATA_DIR):
                    csv_files = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv') and not f.startswith('stock_list')]
                    if csv_files:
                        codes = [f.replace('.csv', '') for f in csv_files]
                        # 60（上海A株）、43、83、87、8（新三板・北交所）で始まるコード
                        codes = [c for c in codes if c.startswith(('60', '43', '83', '87', '8'))]
                        if codes:
                            stocks = pd.DataFrame({
                                "代码": codes,
                                "名称": [f"銘柄{c}" for c in codes]  # 名称は不明なので仮名
                            })
                            print(f"既存データから {len(stocks)}銘柄を取得しました")
                            return stocks
                
                print("エラー: 銘柄リストを取得できませんでした。")
                raise

