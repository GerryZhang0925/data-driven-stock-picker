"""
中国株の収益バリュー株ランキング生成
スクリーニング条件はJSONファイルから読み込む
"""
import json
import os
import pandas as pd
import akshare as ak
from tqdm import tqdm
import time
from config import DATA_DIR_CN, OUTPUT_DIR, TODAY
from stock_list import get_stock_list

# 設定ファイルのパス
CONFIG_FILE = "value_stock_screening_config_cn.json"
OUTPUT_FILE = f"{OUTPUT_DIR}/value_stock_ranking_cn.csv"
FUNDAMENTALS_PATH_CN = f"{DATA_DIR_CN}/fundamentals_cn.csv"


def load_screening_config(config_file=CONFIG_FILE):
    """
    スクリーニング条件をJSONファイルから読み込む
    """
    if not os.path.exists(config_file):
        print(f"警告: 設定ファイル {config_file} が見つかりません。デフォルト設定を使用します。")
        return {
            "operating_margin_min": 10.0,
            "per_max": 10.0,
            "pbr_max": 1.5,
            "roa_min": 7.0,
            "market_cap_max": 300000000000  # 中国株は3000億円（約300億人民元）
        }
    
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        print(f"設定ファイルを読み込みました: {config_file}")
        return config
    except Exception as e:
        print(f"設定ファイルの読み込みに失敗: {str(e)}")
        print("デフォルト設定を使用します。")
        return {
            "operating_margin_min": 10.0,
            "per_max": 10.0,
            "pbr_max": 1.5,
            "roa_min": 7.0,
            "market_cap_max": 300000000000
        }


def get_stock_fundamentals(code, spot_data=None):
    """
    銘柄のファンダメンタル情報を取得（akshare使用）
    複数のAPIを試行して情報を取得
    
    Args:
        code: 銘柄コード
        spot_data: 全銘柄のリアルタイムデータ（PER、PBRを含む）
    """
    fundamentals = {
        "代码": code
    }
    
    try:
        # info_dictを関数スコープで定義（配当利回り計算で使用）
        info_dict = {}
        
        # 方法1: stock_individual_info_emから基本情報を取得
        stock_info = ak.stock_individual_info_em(symbol=code)
        
        if stock_info is not None and not stock_info.empty:
            # データを辞書に変換
            for _, row in stock_info.iterrows():
                if len(row) >= 2:
                    key = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else None
                    value = row.iloc[1] if pd.notna(row.iloc[1]) else None
                    if key and key != "nan":
                        info_dict[key] = value
            
            # デバッグ用：最初の3銘柄で取得データを確認
            if not hasattr(get_stock_fundamentals, '_debug_count'):
                get_stock_fundamentals._debug_count = 0
            get_stock_fundamentals._debug_count += 1
            if get_stock_fundamentals._debug_count <= 3:
                print(f"\n[デバッグ] 銘柄 {code} の取得データ:")
                print(f"  取得キー数: {len(info_dict)}")
                print(f"  主要キー: {list(info_dict.keys())[:20]}")
            
            # 時価総額（总市值）
            market_cap = None
            for key in ["总市值", "市值", "总市值(元)", "流通市值"]:
                if key in info_dict:
                    try:
                        market_cap_str = str(info_dict[key])
                        if "亿" in market_cap_str:
                            market_cap_str = market_cap_str.replace("亿", "").strip()
                            if market_cap_str and market_cap_str != "nan" and market_cap_str != "":
                                market_cap = float(market_cap_str) * 1e8
                        elif "万" in market_cap_str:
                            market_cap_str = market_cap_str.replace("万", "").strip()
                            if market_cap_str and market_cap_str != "nan" and market_cap_str != "":
                                market_cap = float(market_cap_str) * 1e4
                        else:
                            market_cap_str = market_cap_str.replace("元", "").replace(",", "").strip()
                            if market_cap_str and market_cap_str != "nan" and market_cap_str != "":
                                market_cap = float(market_cap_str)
                        break
                    except:
                        continue
            if market_cap is not None:
                fundamentals["時価総額"] = market_cap
            
            # 名称
            name = None
            for key in ["股票简称", "股票名称", "名称", "股票全称"]:
                if key in info_dict:
                    name = str(info_dict[key]).strip()
                    if name and name != "nan":
                        break
            if name:
                fundamentals["名称"] = name
            else:
                fundamentals["名称"] = f"銘柄{code}"
        
        # 方法2: PER、PBRを取得（spot_dataから取得）
        if spot_data is not None and not spot_data.empty:
            try:
                # コードでフィルタ（列名を確認）
                code_col = None
                for col in ['代码', '股票代码', 'code', 'symbol']:
                    if col in spot_data.columns:
                        code_col = col
                        break
                
                if code_col:
                    stock_data = spot_data[spot_data[code_col] == code]
                    if not stock_data.empty:
                        row = stock_data.iloc[0]
                        
                        # PER（市盈率）- 複数の列名を試行
                        per_cols = ['市盈率', '市盈率-动态', '市盈率TTM', 'PE', 'pe', '市盈率(动)']
                        for per_col in per_cols:
                            if per_col in row.index:
                                try:
                                    per_val = row[per_col]
                                    if pd.notna(per_val):
                                        per_str = str(per_val).replace('--', '').replace('-', '').replace('nan', '').replace('None', '').strip()
                                        if per_str and per_str != '':
                                            per_val = float(per_str)
                                            if per_val > 0:
                                                fundamentals["PER"] = per_val
                                                break
                                except:
                                    continue
                        
                        # PBR（市净率）- 複数の列名を試行
                        pbr_cols = ['市净率', 'PB', 'pb', '市净率MRQ', '市净率(动)']
                        for pbr_col in pbr_cols:
                            if pbr_col in row.index:
                                try:
                                    pbr_val = row[pbr_col]
                                    if pd.notna(pbr_val):
                                        pbr_str = str(pbr_val).replace('--', '').replace('-', '').replace('nan', '').replace('None', '').strip()
                                        if pbr_str and pbr_str != '':
                                            pbr_val = float(pbr_str)
                                            if pbr_val > 0:
                                                fundamentals["PBR"] = pbr_val
                                                break
                                except:
                                    continue
                        
                        # デバッグ用：最初の3銘柄でspot_dataの列名を確認
                        if not hasattr(get_stock_fundamentals, '_spot_debug_done'):
                            get_stock_fundamentals._spot_debug_done = True
                            print(f"\n[デバッグ] spot_dataの列名: {list(spot_data.columns)[:20]}")
                            if not stock_data.empty:
                                print(f"[デバッグ] 銘柄 {code} のspot_data行:")
                                print(f"  PER関連列: {[col for col in row.index if '市盈' in str(col) or 'PE' in str(col)]}")
                                print(f"  PBR関連列: {[col for col in row.index if '市净' in str(col) or 'PB' in str(col)]}")
            except Exception as e2:
                pass
        
        # 方法3: 財務分析指標を取得（営業利益率、ROA、ROE、EPS）
        # 注意: このAPIは時間がかかるため、必要に応じてコメントアウト
        try:
            # start_yearパラメータを使用（最近5年分のデータを取得）
            indicators = None
            for year in ["2020", "2019", "2015", "1900"]:
                try:
                    indicators = ak.stock_financial_analysis_indicator(symbol=code, start_year=year)
                    if indicators is not None and not indicators.empty:
                        break
                except Exception:
                    continue
            
            if indicators is not None and not indicators.empty:
                latest = indicators.iloc[-1] if len(indicators) > 0 else None
                if latest is not None:
                    # 営業利益率（营业利润率）
                    for col_name in ["营业利润率", "营业利润率(%)", "销售净利率", "销售净利率(%)"]:
                        if col_name in latest.index:
                            try:
                                op_margin = float(latest[col_name])
                                if not pd.isna(op_margin) and op_margin != 0:
                                    fundamentals["営業利益率"] = op_margin
                                    break
                            except:
                                continue
                    
                    # ROA（总资产报酬率、总资产净利润率）
                    for col_name in ["总资产报酬率", "总资产报酬率(%)", "总资产净利润率(%)", "资产报酬率(%)", "ROA", "资产报酬率", "总资产利润率(%)"]:
                        if col_name in latest.index:
                            try:
                                roa = float(latest[col_name])
                                if not pd.isna(roa) and roa != 0:
                                    fundamentals["ROA"] = roa
                                    break
                            except:
                                continue
                    
                    # ROE（净资产收益率）
                    for col_name in ["净资产收益率", "净资产收益率(%)", "ROE", "加权净资产收益率", "净资产收益率(加权)"]:
                        if col_name in latest.index:
                            try:
                                roe = float(latest[col_name])
                                if not pd.isna(roe) and roe != 0:
                                    fundamentals["ROE"] = roe
                                    break
                            except:
                                continue
                    
                    # EPS（每股收益）
                    for col_name in ["摊薄每股收益(元)", "加权每股收益(元)", "每股收益_调整后(元)", "每股收益", "每股收益(元)", "EPS", "基本每股收益", "每股收益(基本)"]:
                        if col_name in latest.index:
                            try:
                                eps = float(latest[col_name])
                                if not pd.isna(eps) and eps != 0:
                                    fundamentals["EPS"] = eps
                                    break
                            except:
                                continue
        except Exception as e3:
            # 財務指標の取得に失敗しても基本情報は返す
            pass
        
        # 方法4: 配当利回りを取得（stock_dividend_cninfoから計算）
        try:
            from datetime import datetime, timedelta
            
            # 配当データを取得
            dividend_df = ak.stock_dividend_cninfo(symbol=code)
            if dividend_df is not None and not dividend_df.empty:
                # 直近1年分の配当を合計
                one_year_ago = datetime.now() - timedelta(days=365)
                
                # 日付列を探す
                date_cols = [col for col in dividend_df.columns if any(kw in str(col) for kw in ["日期", "公告日", "报告期", "实施", "登记"])]
                if date_cols:
                    try:
                        dt = pd.to_datetime(dividend_df[date_cols[0]], errors="coerce")
                        dividend_df = dividend_df[dt >= one_year_ago]
                    except Exception:
                        pass
                
                # 配当関連の列を探す（派息比例、每股派息など）
                div_cols = [col for col in dividend_df.columns if any(kw in str(col) for kw in ["派", "分红", "股息", "现金", "每股"])]
                if div_cols:
                    for div_col in div_cols:
                        try:
                            div_values = pd.to_numeric(dividend_df[div_col], errors="coerce")
                            if div_values.notna().sum() > 0:
                                div_per_share = float(div_values.dropna().sum())
                                
                                # 株価を取得（info_dictの「最新」を使用）
                                if "最新" in info_dict:
                                    try:
                                        price = float(info_dict["最新"])
                                        if price > 0:
                                            dividend_yield = div_per_share / price * 100
                                            fundamentals["配当利回り"] = dividend_yield
                                            break
                                    except Exception:
                                        pass
                        except Exception:
                            continue
        except Exception as e4:
            pass
        
        # コードと名称以外に情報があれば返す
        if len(fundamentals) > 2:
            return fundamentals
        else:
            return None
        
    except Exception as e:
        # エラー時はNoneを返す
        return None


def get_all_fundamentals(stocks_df=None):
    """
    全銘柄のファンダメンタル情報を取得
    """
    if stocks_df is None:
        stocks_df = get_stock_list()
    
    print(f"\n全{len(stocks_df)}銘柄のファンダメンタル情報を取得中...")
    
    # まず、全銘柄のPER、PBRを一度に取得（効率化）
    print("全銘柄のPER、PBRを取得中...")
    spot_data = None
    try:
        spot_data = ak.stock_zh_a_spot_em()
        if spot_data is not None and not spot_data.empty:
            print(f"PER、PBRデータを取得: {len(spot_data)}銘柄")
            # 列名を確認
            print(f"取得データの列名: {list(spot_data.columns)[:30]}")
            # PER、PBRが含まれているか確認
            per_cols = [col for col in spot_data.columns if '市盈' in str(col) or 'PE' in str(col)]
            pbr_cols = [col for col in spot_data.columns if '市净' in str(col) or 'PB' in str(col)]
            print(f"PER関連列: {per_cols}")
            print(f"PBR関連列: {pbr_cols}")
    except Exception as e:
        print(f"PER、PBRデータの一括取得に失敗: {str(e)}")
    
    fundamentals_list = []
    
    for _, row in tqdm(stocks_df.iterrows(), total=len(stocks_df), desc="ファンダメンタル取得"):
        code = row["代码"]
        fundamentals = get_stock_fundamentals(code, spot_data=spot_data)
        
        if fundamentals:
            fundamentals_list.append(fundamentals)
        
        # APIレート制限対策
        time.sleep(0.05)  # 少し短縮
    
    if not fundamentals_list:
        print("ファンダメンタル情報が取得できませんでした")
        return None
    
    fundamentals_df = pd.DataFrame(fundamentals_list)
    return fundamentals_df


def load_fundamentals(path=None):
    """
    保存済みのファンダメンタル情報を読み込み
    """
    if path is None:
        path = FUNDAMENTALS_PATH_CN
    
    if os.path.exists(path):
        try:
            df = pd.read_csv(path, encoding="utf-8-sig")
            print(f"保存済みファンダメンタル情報を読み込み: {len(df)}銘柄")
            return df
        except Exception as e:
            print(f"ファンダメンタル情報の読み込みに失敗: {str(e)}")
            return None
    return None


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
    
    # スクリーニング条件を適用
    filtered_df = fundamentals_df.copy()
    
    # 営業利益率（取得できる場合）
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
        market_cap_max = config.get('market_cap_max', 300000000000)
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


def main():
    """メイン処理"""
    import argparse
    
    parser = argparse.ArgumentParser(description="中国株の収益バリュー株ランキング生成")
    parser.add_argument("--force-update", action="store_true", help="ファンダメンタル情報を強制的に再取得")
    args = parser.parse_args()
    
    print("=== 中国株 収益バリュー株ランキング生成 ===\n")
    
    # 設定ファイルを読み込み
    config = load_screening_config()
    print(f"\nスクリーニング条件:")
    print(f"  営業利益率: {config.get('operating_margin_min', 10.0)}%以上")
    print(f"  PER: {config.get('per_max', 10.0)}倍以下")
    print(f"  PBR: {config.get('pbr_max', 1.5)}倍以下")
    print(f"  ROA: {config.get('roa_min', 7.0)}%以上")
    print(f"  時価総額: {config.get('market_cap_max', 300000000000) / 1e8:.0f}億円以下\n")
    
    # ファンダメンタル情報を読み込みまたは取得
    print("ファンダメンタル情報を読み込み中...")
    fundamentals_df = load_fundamentals()
    
    # 既存データにPER、PBRなどの情報が含まれているか確認
    needs_update = args.force_update
    if not needs_update and fundamentals_df is not None:
        # 必要な列がすべて含まれているか確認
        required_columns = ['PER', 'PBR']
        has_per_pbr = all(col in fundamentals_df.columns for col in required_columns)
        if not has_per_pbr:
            print("既存データにPER、PBRの情報が含まれていません。")
            print("データを再取得します...")
            needs_update = True
        else:
            # PER、PBRのデータが実際に存在するか確認
            per_count = fundamentals_df['PER'].notna().sum() if 'PER' in fundamentals_df.columns else 0
            pbr_count = fundamentals_df['PBR'].notna().sum() if 'PBR' in fundamentals_df.columns else 0
            if per_count == 0 or pbr_count == 0:
                print(f"既存データにPER、PBRの有効なデータがありません（PER: {per_count}銘柄, PBR: {pbr_count}銘柄）。")
                print("データを再取得します...")
                needs_update = True
    
    if fundamentals_df is None or needs_update:
        if fundamentals_df is None:
            print("保存済みファンダメンタル情報が見つかりません。新規取得を開始します...")
        elif args.force_update:
            print("--force-updateオプションが指定されました。データを再取得します...")
        fundamentals_df = get_all_fundamentals()
        
        if fundamentals_df is not None:
            # 保存
            fundamentals_df.to_csv(FUNDAMENTALS_PATH_CN, index=False, encoding="utf-8-sig")
            print(f"ファンダメンタル情報を保存しました: {FUNDAMENTALS_PATH_CN}")
    
    if fundamentals_df is None or fundamentals_df.empty:
        print("エラー: ファンダメンタル情報が取得できませんでした。")
        return
    
    print(f"読み込み完了: {len(fundamentals_df)}銘柄")
    
    # データの有無を確認
    if 'PER' in fundamentals_df.columns:
        per_count = fundamentals_df['PER'].notna().sum()
        print(f"  PERデータあり: {per_count}銘柄")
    else:
        print(f"  PERデータ: 列が存在しません")
    if 'PBR' in fundamentals_df.columns:
        pbr_count = fundamentals_df['PBR'].notna().sum()
        print(f"  PBRデータあり: {pbr_count}銘柄")
    else:
        print(f"  PBRデータ: 列が存在しません")
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
    ranking_df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    print(f"ランキングを保存しました: {OUTPUT_FILE}")
    print(f"  対象銘柄数: {len(ranking_df)}銘柄\n")
    
    # 上位10銘柄を表示
    print("【収益バリュー株ランキング 上位10銘柄】")
    display_df = ranking_df.head(10).copy()
    # 時価総額列を削除（億円版のみ表示）
    if '時価総額' in display_df.columns and '時価総額（億円）' in display_df.columns:
        display_df = display_df.drop('時価総額', axis=1)
    print(display_df.to_string(index=False))


if __name__ == "__main__":
    main()
