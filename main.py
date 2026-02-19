"""
メイン処理
"""
import argparse
import pandas as pd
from tqdm import tqdm
import os
import json
import time
import akshare as ak

from config import DATA_DIR_CN, OUTPUT_DIR, TODAY, MA_WINDOW
from stock_list import get_stock_list
from cn.data_loader import load_or_download, get_latest_trading_date
from volume_analyzer import detect_volume_spike
from output import (
    print_data_acquisition_summary,
    save_failed_stocks,
    save_old_data_stocks,
    retry_failed_stocks,
    save_ranking_results
)

# 収益バリュー株ランキング用の設定
CONFIG_FILE_CN = "value_stock_screening_config_cn.json"
OUTPUT_FILE_VALUE_CN = f"{OUTPUT_DIR}/value_stock_ranking_cn.csv"
FUNDAMENTALS_PATH_CN = f"{DATA_DIR_CN}/fundamentals_cn.csv"


def main(no_download=False, force_update=False):
    """メイン処理"""
    # オプション表示
    if no_download:
        print("【モード: 既存データのみ使用】データ取得をスキップして既存データで計算します")
    elif force_update:
        print("【モード: 強制更新】すべての銘柄のデータを強制的に更新します")
    
    # 銘柄リストを取得
    stocks = get_stock_list()
    
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
            path = f"{DATA_DIR_CN}/{code}.csv"
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
                        stats_by_type[stock_type]["failed"] += 1
                        continue
                else:
                    failed_stocks.append({"code": code, "name": name, "reason": "既存データが存在しません"})
                    stats_by_type[stock_type]["failed"] += 1
                    continue
            else:
                df = load_or_download(code, latest_trading_date=latest_trading_date, force_update=force_update)
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
    if failed_stocks and not no_download:  # no_download時は再試行をスキップ
        retry_failed_stocks(failed_stocks, lambda code: load_or_download(code, latest_trading_date=latest_trading_date, force_update=force_update))
    
    # ランキング結果を保存
    save_ranking_results(ratio_results, z_results)
    
    # 収益バリュー株ランキングを生成
    generate_value_stock_ranking()


def load_screening_config(config_file=CONFIG_FILE_CN):
    """
    スクリーニング条件をJSONファイルから読み込む
    """
    if not os.path.exists(config_file):
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
    """
    fundamentals = {
        "代码": code
    }
    
    # デバッグ用カウンター
    if not hasattr(get_stock_fundamentals, '_debug_count'):
        get_stock_fundamentals._debug_count = 0
    
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
            
            # デバッグ: 最初の3銘柄でinfo_dictの内容を表示（簡略版）
            if not hasattr(get_stock_fundamentals, '_debug_count'):
                get_stock_fundamentals._debug_count = 0
            if get_stock_fundamentals._debug_count <= 3:
                print(f"  [デバッグ] info_dictキー数={len(info_dict)}, 主要キー={list(info_dict.keys())[:5]}")
        
        # 方法2: PER、PBRを取得（spot_dataから取得）
        if spot_data is not None and not spot_data.empty:
            try:
                code_col = None
                for col in ['代码', '股票代码', 'code', 'symbol']:
                    if col in spot_data.columns:
                        code_col = col
                        break
                
                if code_col:
                    # コードの型を統一（文字列として比較）
                    stock_data = spot_data[spot_data[code_col].astype(str) == str(code)]
                    if not stock_data.empty:
                        row = stock_data.iloc[0]
                        
                        # PER（市盈率）
                        per_cols = ['市盈率', '市盈率-动态', '市盈率TTM', 'PE', 'pe', '市盈率(动)', '市盈率-动态', '市盈率（动）']
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
                        
                        # PBR（市净率）
                        pbr_cols = ['市净率', 'PB', 'pb', '市净率MRQ', '市净率(动)', '市净率（动）']
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
            except Exception as e2:
                pass
        
        # 方法3: 財務分析指標を取得（営業利益率、ROA、ROE、EPS）
        # 注意: このAPIは時間がかかるため、必要に応じてスキップ
        try:
            # start_yearパラメータを使用（最近5年分のデータを取得）
            indicators = None
            for year in ["2020", "2019", "2015", "1900"]:
                try:
                    # タイムアウトを設定（10秒）
                    import signal
                    indicators = ak.stock_financial_analysis_indicator(symbol=code, start_year=year)
                    if indicators is not None and not indicators.empty:
                        break
                except Exception as e:
                    # エラーが発生した場合は次の年を試す
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
            pass
        
        # 方法4: 配当利回りを取得（stock_dividend_cninfoから計算）
        try:
            from datetime import datetime, timedelta
            # 注意: akは既にファイルの先頭でインポートされているため、再インポート不要
            
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
                                
                                # 株価を取得（stock_individual_info_emの「最新」を使用）
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
        
        # デバッグ: 最初の3銘柄で取得内容を表示（簡略版）
        if not hasattr(get_stock_fundamentals, '_debug_count'):
            get_stock_fundamentals._debug_count = 0
        get_stock_fundamentals._debug_count += 1
        if get_stock_fundamentals._debug_count <= 3:
            keys = list(fundamentals.keys())
            print(f"\n[デバッグ] 銘柄 {code}: 取得キー={keys}, 数={len(keys)}")
        
        # 返却条件: 名称が取得できていれば、他の情報がなくても返す
        # （PER/PBRは後でspot_dataから取得できる可能性があるため）
        if "名称" in fundamentals:
            # 名称があれば返す（最低限の情報として）
            return fundamentals
        else:
            # 名称も取得できなかった場合はNoneを返す
            if get_stock_fundamentals._debug_count <= 3:
                print(f"  → 名称が取得できなかったためNoneを返します")
            return None
        
    except Exception as e:
        if get_stock_fundamentals._debug_count <= 5:
            print(f"\n[デバッグ] 銘柄 {code} でエラー: {str(e)}")
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
            # デバッグ: 列名を確認
            print(f"[デバッグ] spot_dataの列名: {list(spot_data.columns)[:20]}")
            # コード列を確認
            code_cols = [col for col in spot_data.columns if any(kw in str(col) for kw in ['代码', 'code', 'symbol'])]
            print(f"[デバッグ] コード列候補: {code_cols}")
            if code_cols:
                sample_codes = spot_data[code_cols[0]].head(5).tolist()
                print(f"[デバッグ] サンプルコード: {sample_codes}")
    except Exception as e:
        print(f"PER、PBRデータの一括取得に失敗: {str(e)}")
    
    fundamentals_list = []
    failed_count = 0
    success_count = 0
    
    for idx, (_, row) in enumerate(tqdm(stocks_df.iterrows(), total=len(stocks_df), desc="ファンダメンタル取得")):
        code = row["代码"]
        
        # 進捗表示（100銘柄ごと）
        if (idx + 1) % 100 == 0:
            print(f"\n[進捗] {idx + 1}/{len(stocks_df)}銘柄処理完了 (成功: {success_count}, 失敗: {failed_count})")
        
        fundamentals = get_stock_fundamentals(code, spot_data=spot_data)
        
        if fundamentals:
            fundamentals_list.append(fundamentals)
            success_count += 1
        else:
            failed_count += 1
            # 最初の5件の失敗例のみ表示
            if failed_count <= 5:
                print(f"\n[デバッグ] 銘柄 {code} のファンダメンタル情報取得に失敗")
        
        # APIレート制限対策（財務分析指標の取得に時間がかかるため、少し長めに）
        time.sleep(0.1)
    
    print(f"\n[デバッグ] ファンダメンタル情報取得結果: 成功 {success_count}銘柄, 失敗 {failed_count}銘柄")
    
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


def generate_value_stock_ranking():
    """
    収益バリュー株ランキングを生成
    """
    print("\n" + "="*50)
    print("=== 中国株 収益バリュー株ランキング生成 ===")
    print("="*50 + "\n")
    
    # 設定ファイルを読み込み
    config = load_screening_config()
    print(f"スクリーニング条件:")
    print(f"  営業利益率: {config.get('operating_margin_min', 10.0)}%以上")
    print(f"  PER: {config.get('per_max', 10.0)}倍以下")
    print(f"  PBR: {config.get('pbr_max', 1.5)}倍以下")
    print(f"  ROA: {config.get('roa_min', 7.0)}%以上")
    print(f"  時価総額: {config.get('market_cap_max', 300000000000) / 1e8:.0f}億円以下\n")
    
    # ファンダメンタル情報を読み込み
    print("ファンダメンタル情報を読み込み中...")
    fundamentals_df = load_fundamentals()
    
    # 既存データに必要な情報が含まれているか確認
    needs_update = False
    if fundamentals_df is not None:
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
            else:
                # 営業利益率、ROA、ROE、EPS、配当利回りの有無を確認
                optional_columns = ['営業利益率', 'ROA', 'ROE', 'EPS', '配当利回り']
                missing_columns = [col for col in optional_columns if col not in fundamentals_df.columns]
                if missing_columns:
                    print(f"既存データに以下の情報が含まれていません: {', '.join(missing_columns)}")
                    print("データを再取得します...")
                    needs_update = True
                else:
                    # 各列にデータが存在するか確認
                    op_margin_count = fundamentals_df['営業利益率'].notna().sum() if '営業利益率' in fundamentals_df.columns else 0
                    roa_count = fundamentals_df['ROA'].notna().sum() if 'ROA' in fundamentals_df.columns else 0
                    if op_margin_count == 0 or roa_count == 0:
                        print(f"既存データに営業利益率、ROAの有効なデータがありません（営業利益率: {op_margin_count}銘柄, ROA: {roa_count}銘柄）。")
                        print("データを再取得します...")
                        needs_update = True
    
    if fundamentals_df is None or needs_update:
        if fundamentals_df is None:
            print("保存済みファンダメンタル情報が見つかりません。新規取得を開始します...")
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
    ranking_df.to_csv(OUTPUT_FILE_VALUE_CN, index=False, encoding="utf-8-sig")
    print(f"ランキングを保存しました: {OUTPUT_FILE_VALUE_CN}")
    print(f"  対象銘柄数: {len(ranking_df)}銘柄\n")
    
    # 上位10銘柄を表示
    print("【収益バリュー株ランキング 上位10銘柄】")
    display_df = ranking_df.head(10).copy()
    # 時価総額列を削除（億円版のみ表示）
    if '時価総額' in display_df.columns and '時価総額（億円）' in display_df.columns:
        display_df = display_df.drop('時価総額', axis=1)
    print(display_df.to_string(index=False))


# メイン処理を実行
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="中国株の出来高急増検知システム",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  python main.py                    # 通常モード（必要に応じてデータを更新）
  python main.py --no-download      # 既存データのみを使用（データ取得をスキップ）
  python main.py --force-update     # 強制更新モード（すべての銘柄のデータを更新）
  python main.py --help             # このヘルプメッセージを表示

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

