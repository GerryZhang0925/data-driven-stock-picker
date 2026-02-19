"""
中国株のファンダメンタル情報取得テストスクリプト
営業利益率、ROA、ROE、配当利回り、EPSの取得可否を確認
"""
import pandas as pd
import akshare as ak
import sys
import os
import inspect
from datetime import datetime, timedelta
import time
from stock_list import get_stock_list

# テスト用のサンプル銘柄（異なる市場から選択）
TEST_STOCKS = [
    "600000",  # 上海A株
    "600519",  # 上海A株（有名銘柄）
    "688111",  # 科創板
    "688147",  # 科創板
    "830946",  # 新三板
]


def _print_env_info():
    """実行環境情報を表示（ユーザー環境でakshareの差異を把握するため）"""
    print("\n[環境情報]")
    print(f"  python: {sys.version.split()[0]}")
    print(f"  executable: {sys.executable}")
    print(f"  cwd: {os.getcwd()}")
    print(f"  akshare: {getattr(ak, '__version__', 'unknown')}")


def _candidate_symbols(code: str) -> list[str]:
    """
    akshareのAPIは銘柄コードの形式が関数ごとに異なる場合があるため、候補を複数生成する。
    - Eastmoney系: '600000' のことが多い
    - Sina系など: 'sh600000' / 'sz000001' のことがある
    """
    code = str(code).strip()
    if not code:
        return []

    # 上場市場推定（厳密ではないがテスト用途）
    is_sh = code.startswith(("6", "9", "68"))
    prefix = "sh" if is_sh else "sz"

    return [
        code,
        f"{prefix}{code}",
        f"{prefix.upper()}{code}",
        f"{code}.{'SH' if is_sh else 'SZ'}",
    ]


def _call_first_dataframe(func, *, code: str, indicator_candidates: list[str] | None = None):
    """
    関数シグネチャを見ながら、取りうる引数で順に呼び出してDataFrameを得る。
    戻り値がDataFrameでない/空の場合は次を試す。
    """
    sig = None
    try:
        sig = inspect.signature(func)
    except Exception:
        sig = None

    param_names = list(sig.parameters.keys()) if sig else []

    # kwargs候補（symbol/stock/code）
    kw_keys = [k for k in ["symbol", "stock", "code"] if k in param_names]
    # indicator候補（存在する場合）
    has_indicator_kw = "indicator" in param_names

    attempts = []
    for s in _candidate_symbols(code):
        # 1) kwargsで symbol/stock/code を試す
        for key in kw_keys:
            if has_indicator_kw and indicator_candidates:
                for ind in indicator_candidates:
                    attempts.append(("kwargs", {"__key__": key, "__sym__": s, "indicator": ind}))
            attempts.append(("kwargs", {"__key__": key, "__sym__": s}))

        # 2) 位置引数（最小）
        if len(param_names) >= 1:
            if indicator_candidates and len(param_names) >= 2:
                for ind in indicator_candidates:
                    attempts.append(("pos", (s, ind)))
            attempts.append(("pos", (s,)))

    seen = set()
    for kind, payload in attempts:
        try:
            if kind == "kwargs":
                key = payload["__key__"]
                sym = payload["__sym__"]
                kwargs = {key: sym}
                if "indicator" in payload:
                    kwargs["indicator"] = payload["indicator"]
                sig_key = ("kwargs", key, sym, kwargs.get("indicator"))
                if sig_key in seen:
                    continue
                seen.add(sig_key)
                df = func(**kwargs)
                if isinstance(df, pd.DataFrame) and (not df.empty):
                    return df, f"{func.__name__}({key}={sym}" + (f", indicator={kwargs.get('indicator')}" if "indicator" in kwargs else "") + ")"
            else:
                args = payload
                sig_key = ("pos",) + tuple(args)
                if sig_key in seen:
                    continue
                seen.add(sig_key)
                df = func(*args)
                if isinstance(df, pd.DataFrame) and (not df.empty):
                    return df, f"{func.__name__}({', '.join(map(str, args))})"
        except Exception:
            continue

    return None, None


def _find_ak_functions(substrings: list[str]) -> list[str]:
    """akshare内の関数名から候補を抽出"""
    names = []
    for name in dir(ak):
        lname = name.lower()
        if any(sub in lname for sub in substrings) and callable(getattr(ak, name, None)):
            names.append(name)
    # 再現性のためソート
    return sorted(set(names))


def _extract_numeric(series: pd.Series, col_candidates: list[str]):
    for c in col_candidates:
        if c in series.index:
            try:
                v = pd.to_numeric(series[c], errors="coerce")
                if pd.notna(v):
                    return float(v), c
            except Exception:
                continue
    return None, None


def _extract_column(df: pd.DataFrame, col_keywords: list[str]) -> list[str]:
    cols = []
    for c in df.columns:
        s = str(c)
        if any(k in s for k in col_keywords):
            cols.append(c)
    return cols


def test_stock_individual_info_em(code):
    """stock_individual_info_emから取得できる情報をテスト"""
    print(f"\n{'='*60}")
    print(f"【テスト1】 stock_individual_info_em - 銘柄コード: {code}")
    print(f"{'='*60}")
    
    try:
        stock_info = ak.stock_individual_info_em(symbol=code)
        
        if stock_info is None or stock_info.empty:
            print("❌ データが取得できませんでした")
            return {}
        
        # データを辞書に変換
        info_dict = {}
        for _, row in stock_info.iterrows():
            if len(row) >= 2:
                key = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else None
                value = row.iloc[1] if pd.notna(row.iloc[1]) else None
                if key and key != "nan":
                    info_dict[key] = value
        
        print(f"✅ 取得できたキー数: {len(info_dict)}")
        print(f"\n取得できたキー一覧:")
        for i, key in enumerate(list(info_dict.keys())[:30], 1):
            value = info_dict[key]
            if isinstance(value, (int, float)):
                print(f"  {i:2d}. {key}: {value}")
            else:
                value_str = str(value)[:50] if value else "None"
                print(f"  {i:2d}. {key}: {value_str}")
        
        # 目的の情報を探す
        result = {}
        
        # 営業利益率
        for key in ["营业利润率", "营业利润率(%)", "销售净利率", "营业利润", "营业利润(元)"]:
            if key in info_dict:
                result["営業利益率"] = info_dict[key]
                print(f"\n✅ 営業利益率: {info_dict[key]} (キー: {key})")
                break
        
        # ROA
        for key in ["总资产报酬率", "总资产报酬率(%)", "ROA", "资产报酬率"]:
            if key in info_dict:
                result["ROA"] = info_dict[key]
                print(f"✅ ROA: {info_dict[key]} (キー: {key})")
                break
        
        # ROE
        for key in ["净资产收益率", "净资产收益率(%)", "ROE", "净资产收益率(加权)"]:
            if key in info_dict:
                result["ROE"] = info_dict[key]
                print(f"✅ ROE: {info_dict[key]} (キー: {key})")
                break
        
        # 配当利回り
        for key in ["股息率", "股息率(%)", "分红率", "分红率(%)", "股息", "股息(元)"]:
            if key in info_dict:
                result["配当利回り"] = info_dict[key]
                print(f"✅ 配当利回り: {info_dict[key]} (キー: {key})")
                break
        
        # EPS
        for key in ["每股收益", "每股收益(元)", "EPS", "基本每股收益", "每股收益(基本)"]:
            if key in info_dict:
                result["EPS"] = info_dict[key]
                print(f"✅ EPS: {info_dict[key]} (キー: {key})")
                break
        
        # 取得できなかった情報を表示
        missing = []
        if "営業利益率" not in result:
            missing.append("営業利益率")
        if "ROA" not in result:
            missing.append("ROA")
        if "ROE" not in result:
            missing.append("ROE")
        if "配当利回り" not in result:
            missing.append("配当利回り")
        if "EPS" not in result:
            missing.append("EPS")
        
        if missing:
            print(f"\n❌ 取得できなかった情報: {', '.join(missing)}")
        
        return result
        
    except Exception as e:
        print(f"❌ エラー: {str(e)}")
        return {}


def test_stock_financial_analysis_indicator(code):
    """stock_financial_analysis_indicatorから取得できる情報をテスト"""
    print(f"\n{'='*60}")
    print(f"【テスト2】 stock_financial_analysis_indicator - 銘柄コード: {code}")
    print(f"{'='*60}")

    if not hasattr(ak, "stock_financial_analysis_indicator"):
        print("⚠️  ak.stock_financial_analysis_indicator が見つかりません")
        return {}

    func = ak.stock_financial_analysis_indicator
    try:
        print(f"関数シグネチャ: {inspect.signature(func)}")
    except Exception:
        pass

    # シグネチャから正しいパラメータを確認
    import inspect
    sig = inspect.signature(func)
    params = list(sig.parameters.keys())
    
    # start_yearパラメータがある場合はそれを使う
    if 'start_year' in params:
        # 最近5年分のデータを取得
        year_candidates = ["2020", "2019", "2015", "1900"]  # デフォルトは1900
        df = None
        used = None
        for year in year_candidates:
            try:
                df = func(symbol=code, start_year=year)
                if df is not None and not df.empty:
                    used = f"stock_financial_analysis_indicator(symbol={code}, start_year={year})"
                    break
            except Exception:
                continue
    else:
        # シグネチャにstart_yearがない場合は、symbolのみで試行
        try:
            df = func(symbol=code)
            used = f"stock_financial_analysis_indicator(symbol={code})"
        except Exception:
            df = None
            used = None
    if df is None or df.empty:
        print("❌ データが取得できませんでした（戻りが空/呼び出し失敗）")
        print("  ※このAPIが空の場合、別API（利润表/资产负债表）から計算で作る必要があります")
        return {}

    print(f"✅ データ取得成功: {len(df)}行 via {used}")
    print("列名（先頭30）:", list(df.columns)[:30])

    latest = df.iloc[-1]
    result = {}

    op, op_col = _extract_numeric(latest, ["营业利润率", "营业利润率(%)", "销售净利率", "销售净利率(%)"])
    if op is not None:
        result["営業利益率"] = op
        print(f"✅ 営業利益率: {op} (列: {op_col})")

    # ROAの候補列名を拡張（テスト結果から判明した列名も追加）
    roa, roa_col = _extract_numeric(latest, ["总资产报酬率", "总资产报酬率(%)", "总资产净利润率(%)", "资产报酬率(%)", "ROA", "资产报酬率", "总资产利润率(%)"])
    if roa is not None:
        result["ROA"] = roa
        print(f"✅ ROA: {roa} (列: {roa_col})")

    roe, roe_col = _extract_numeric(latest, ["净资产收益率", "净资产收益率(%)", "ROE", "加权净资产收益率", "净资产收益率(加权)"])
    if roe is not None:
        result["ROE"] = roe
        print(f"✅ ROE: {roe} (列: {roe_col})")

    # EPSの候補列名を拡張（テスト結果から判明した列名も追加）
    eps, eps_col = _extract_numeric(latest, ["摊薄每股收益(元)", "加权每股收益(元)", "每股收益_调整后(元)", "每股收益", "每股收益(元)", "EPS", "基本每股收益", "每股收益(基本)"])
    if eps is not None:
        result["EPS"] = eps
        print(f"✅ EPS: {eps} (列: {eps_col})")

    divy, divy_col = _extract_numeric(latest, ["股息率", "股息率(%)", "分红率", "分红率(%)"])
    if divy is not None:
        result["配当利回り"] = divy
        print(f"✅ 配当利回り: {divy} (列: {divy_col})")

    if not result:
        print("❌ 指定の指標列が見つかりませんでした（列名/版差異の可能性）")

    return result


def test_stock_zh_a_spot_em(code):
    """stock_zh_a_spot_emから取得できる情報をテスト"""
    print(f"\n{'='*60}")
    print(f"【テスト3】 stock_zh_a_spot_em - 銘柄コード: {code}")
    print(f"{'='*60}")
    
    try:
        # 全銘柄のデータを取得
        spot_data = ak.stock_zh_a_spot_em()
        
        if spot_data is None or spot_data.empty:
            print("❌ データが取得できませんでした")
            return {}
        
        print(f"✅ データ取得成功: {len(spot_data)}銘柄")
        
        # コード列を特定
        code_col = None
        for col in ['代码', '股票代码', 'code', 'symbol']:
            if col in spot_data.columns:
                code_col = col
                break
        
        if not code_col:
            print("❌ コード列が見つかりません")
            return {}
        
        # 該当銘柄のデータを取得
        stock_data = spot_data[spot_data[code_col] == code]
        
        if stock_data.empty:
            print(f"❌ 銘柄コード {code} のデータが見つかりません")
            return {}
        
        row = stock_data.iloc[0]
        
        print(f"\n列名一覧:")
        for i, col in enumerate(list(spot_data.columns)[:30], 1):
            print(f"  {i:2d}. {col}")
        
        result = {}
        
        # 営業利益率、ROA、ROE、配当利回り、EPSは通常このAPIには含まれていない
        # ただし、確認のため検索
        print(f"\n検索結果:")
        
        # 営業利益率
        for col in row.index:
            if '营业利润' in str(col) or '利润率' in str(col):
                try:
                    value = row[col]
                    if pd.notna(value):
                        result["営業利益率"] = value
                        print(f"✅ 営業利益率関連: {col} = {value}")
                        break
                except:
                    continue
        
        # ROA
        for col in row.index:
            if 'ROA' in str(col) or '资产报酬' in str(col):
                try:
                    value = row[col]
                    if pd.notna(value):
                        result["ROA"] = value
                        print(f"✅ ROA関連: {col} = {value}")
                        break
                except:
                    continue
        
        # ROE
        for col in row.index:
            if 'ROE' in str(col) or '净资产收益' in str(col):
                try:
                    value = row[col]
                    if pd.notna(value):
                        result["ROE"] = value
                        print(f"✅ ROE関連: {col} = {value}")
                        break
                except:
                    continue
        
        # 配当利回り
        for col in row.index:
            if '股息' in str(col) or '分红' in str(col) or '股息率' in str(col):
                try:
                    value = row[col]
                    if pd.notna(value):
                        result["配当利回り"] = value
                        print(f"✅ 配当利回り関連: {col} = {value}")
                        break
                except:
                    continue
        
        # EPS
        for col in row.index:
            if '每股收益' in str(col) or 'EPS' in str(col):
                try:
                    value = row[col]
                    if pd.notna(value):
                        result["EPS"] = value
                        print(f"✅ EPS関連: {col} = {value}")
                        break
                except:
                    continue
        
        if not result:
            print("❌ このAPIからは営業利益率、ROA、ROE、配当利回り、EPSは取得できません")
        
        return result
        
    except Exception as e:
        print(f"❌ エラー: {str(e)}")
        import traceback
        traceback.print_exc()
        return {}


def test_other_apis(code):
    """その他のAPI（財務諸表・配当）を探索して、計算で指標が作れるかをテスト"""
    print(f"\n{'='*60}")
    print(f"【テスト4】 その他のAPI - 銘柄コード: {code}")
    print(f"{'='*60}")

    # 1) 利益表/资产负债表/配当系の関数を探索
    profit_funcs = _find_ak_functions(["profit_sheet", "income", "profit"])
    balance_funcs = _find_ak_functions(["balance_sheet", "balance"])
    dividend_funcs = _find_ak_functions(["dividend", "bonus", "fenhong"])

    print(f"[探索] 利益表候補: {len(profit_funcs)}件（例: {profit_funcs[:5]}）")
    print(f"[探索] 資産負債表候補: {len(balance_funcs)}件（例: {balance_funcs[:5]}）")
    print(f"[探索] 配当候補: {len(dividend_funcs)}件（例: {dividend_funcs[:5]}）")

    # 2) 利益表（EPS/営業利益/売上）を1つ取る
    profit_df = None
    profit_used = None
    for name in profit_funcs[:20]:
        func = getattr(ak, name, None)
        if not callable(func):
            continue
        df, used = _call_first_dataframe(func, code=code)
        if df is not None and not df.empty:
            profit_df = df
            profit_used = used
            break

    # 3) 資産負債表（総資産/純資産）を1つ取る
    balance_df = None
    balance_used = None
    for name in balance_funcs[:20]:
        func = getattr(ak, name, None)
        if not callable(func):
            continue
        df, used = _call_first_dataframe(func, code=code)
        if df is not None and not df.empty:
            balance_df = df
            balance_used = used
            break

    # 4) 配当データを1つ取る
    dividend_df = None
    dividend_used = None
    for name in dividend_funcs[:20]:
        func = getattr(ak, name, None)
        if not callable(func):
            continue
        df, used = _call_first_dataframe(func, code=code)
        if df is not None and not df.empty:
            dividend_df = df
            dividend_used = used
            break

    results = {}

    # ---- EPS（利益表） ----
    if profit_df is not None:
        print(f"\n✅ 利益表データ取得: {len(profit_df)}行 via {profit_used}")
        print(f"  列名（先頭20）: {list(profit_df.columns)[:20]}")

        last = profit_df.iloc[-1]
        eps_val, eps_col = _extract_numeric(last, ["基本每股收益", "每股收益", "每股收益(元)", "EPS", "稀释每股收益"])
        if eps_val is not None:
            results["EPS"] = {"source": profit_used, "value": eps_val, "column": eps_col}
            print(f"✅ EPS: {eps_val} (列: {eps_col})")
        else:
            # 列名ヒント
            eps_cols = _extract_column(profit_df, ["每股", "EPS"])
            print(f"❌ EPS列が見つかりません。候補列: {eps_cols[:10]}")

        # 営業利益率（営業利益/売上）を計算できるか確認
        # 英語列名も検索
        rev, rev_col = _extract_numeric(last, ["营业总收入", "营业收入", "主营业务收入", "营业收入(元)", "OPERATING_REVENUE", "REVENUE", "TOTAL_REVENUE", "OPERATING_INCOME"])
        op, op_col = _extract_numeric(last, ["营业利润", "营业利润(元)", "营业利润总额", "营业利润总额(元)", "OPERATING_PROFIT", "OPERATING_INCOME", "OPERATING_PROFIT_LOSS"])
        if rev is not None and op is not None and rev != 0:
            op_margin = op / rev * 100
            results["営業利益率(計算)"] = {"source": profit_used, "value": op_margin, "inputs": {"revenue": (rev, rev_col), "op_profit": (op, op_col)}}
            print(f"✅ 営業利益率(計算): {op_margin:.2f}%（营业利润/营业收入）")
        else:
            print("❌ 営業利益率の計算に必要な列が不足（营业收入/营业利润が見つからない可能性）")

    else:
        print("\n❌ 利益表データが取得できませんでした（profit系APIが見つからない/空）")

    # ---- ROA/ROE（利益表＋資産負債表で計算） ----
    if profit_df is not None and balance_df is not None:
        print(f"\n✅ 資産負債表データ取得: {len(balance_df)}行 via {balance_used}")
        print(f"  列名（先頭20）: {list(balance_df.columns)[:20]}")

        p_last = profit_df.iloc[-1]
        b_last = balance_df.iloc[-1]

        # 英語列名も検索
        net, net_col = _extract_numeric(p_last, ["净利润", "归属于母公司股东的净利润", "净利润(元)", "NET_PROFIT", "NET_INCOME", "PROFIT_ATTRIBUTABLE_TO_PARENT"])
        assets, assets_col = _extract_numeric(b_last, ["资产总计", "总资产", "资产总额", "TOTAL_ASSETS", "ASSETS", "ASSET_TOTAL"])
        equity, equity_col = _extract_numeric(b_last, ["所有者权益(或股东权益)合计", "所有者权益合计", "股东权益合计", "净资产", "TOTAL_EQUITY", "EQUITY", "SHAREHOLDERS_EQUITY", "OWNERS_EQUITY"])

        if net is not None and assets is not None and assets != 0:
            roa = net / assets * 100
            results["ROA(計算)"] = {"source": f"{profit_used} + {balance_used}", "value": roa, "inputs": {"net_profit": (net, net_col), "assets": (assets, assets_col)}}
            print(f"✅ ROA(計算): {roa:.2f}%（净利润/总资产）")
        else:
            print("❌ ROAの計算に必要な列が不足（净利润/总资产）")

        if net is not None and equity is not None and equity != 0:
            roe = net / equity * 100
            results["ROE(計算)"] = {"source": f"{profit_used} + {balance_used}", "value": roe, "inputs": {"net_profit": (net, net_col), "equity": (equity, equity_col)}}
            print(f"✅ ROE(計算): {roe:.2f}%（净利润/股东权益）")
        else:
            print("❌ ROEの計算に必要な列が不足（净利润/股东权益）")

    elif balance_df is None:
        print("\n❌ 資産負債表データが取得できませんでした（balance系APIが見つからない/空）")

    # ---- 配当利回り（配当データ＋株価で計算） ----
    # 株価はstock_individual_info_emの「最新」を利用（取れることが多い）
    last_price = None
    try:
        info = ak.stock_individual_info_em(symbol=str(code))
        if isinstance(info, pd.DataFrame) and not info.empty:
            info_dict = {str(r.iloc[0]).strip(): r.iloc[1] for _, r in info.iterrows() if len(r) >= 2}
            if "最新" in info_dict:
                last_price = pd.to_numeric(info_dict["最新"], errors="coerce")
                if pd.notna(last_price):
                    last_price = float(last_price)
    except Exception:
        pass

    if dividend_df is not None:
        print(f"\n✅ 配当データ取得: {len(dividend_df)}行 via {dividend_used}")
        print(f"  列名（先頭30）: {list(dividend_df.columns)[:30]}")

        # 「每股派息」「派息」「现金分红」などを探す（版差異が大きいのでキーワードで探索）
        div_cols = _extract_column(dividend_df, ["派", "分红", "股息", "现金", "每股"])
        print(f"  配当関連の候補列: {div_cols[:15]}")

        # 直近1年分を雑に合計する（列名が分からない場合もあるので、最初の数値列を優先）
        one_year_ago = datetime.now() - timedelta(days=365)

        df_tmp = dividend_df.copy()
        date_cols = _extract_column(df_tmp, ["日期", "公告日", "报告期", "实施", "登记"])
        if date_cols:
            # 先頭の日時列でフィルタを試みる
            dc = date_cols[0]
            try:
                dt = pd.to_datetime(df_tmp[dc], errors="coerce")
                df_tmp = df_tmp[dt >= one_year_ago]
            except Exception:
                pass

        # 数値化しやすい列を探して合計
        div_per_share = None
        div_col_used = None
        for c in div_cols:
            try:
                s = pd.to_numeric(df_tmp[c], errors="coerce")
                if s.notna().sum() > 0:
                    div_per_share = float(s.dropna().sum())
                    div_col_used = c
                    break
            except Exception:
                continue

        if div_per_share is not None:
            results["配当(1年合計:推定)"] = {"source": dividend_used, "value": div_per_share, "column": str(div_col_used)}
            print(f"✅ 配当(1年合計:推定): {div_per_share} (列: {div_col_used})")

            if last_price is not None and last_price > 0:
                dy = div_per_share / last_price * 100
                results["配当利回り(計算:推定)"] = {"source": f"{dividend_used} + stock_individual_info_em", "value": dy, "inputs": {"div_per_share": div_per_share, "price": last_price}}
                print(f"✅ 配当利回り(計算:推定): {dy:.2f}%（配当/株価）")
            else:
                print("⚠️  株価（最新）が取れないため、配当利回りの計算はスキップ")
        else:
            print("❌ 配当の数値列が特定できませんでした（列名/形式が異なる可能性）")

    else:
        print("\n❌ 配当データが取得できませんでした（dividend系APIが見つからない/空）")

    if not results:
        print("\n❌ 結果: 財務諸表/配当からも指標を構築できませんでした（API不足/列名差異の可能性）")
    return results


def main():
    """メイン処理"""
    print("="*60)
    print("中国株 ファンダメンタル情報取得テスト")
    print("="*60)
    _print_env_info()
    print("\nテスト対象:")
    print("  - 営業利益率")
    print("  - ROA（総資産利益率）")
    print("  - ROE（自己資本利益率）")
    print("  - 配当利回り")
    print("  - EPS（1株当たり利益）")
    print(f"\nテスト銘柄: {', '.join(TEST_STOCKS)}")
    
    # 全銘柄の結果を集計
    all_results = {
        "stock_individual_info_em": {},
        "stock_financial_analysis_indicator": {},
        "stock_zh_a_spot_em": {},
        "other_apis": {}
    }
    
    for code in TEST_STOCKS:
        print(f"\n\n{'#'*60}")
        print(f"# 銘柄コード: {code}")
        print(f"{'#'*60}")
        
        # テスト1: stock_individual_info_em
        result1 = test_stock_individual_info_em(code)
        if result1:
            for key, value in result1.items():
                if key not in all_results["stock_individual_info_em"]:
                    all_results["stock_individual_info_em"][key] = []
                all_results["stock_individual_info_em"][key].append(f"{code}: {value}")
        
        time.sleep(1)  # APIレート制限対策
        
        # テスト2: stock_financial_analysis_indicator
        result2 = test_stock_financial_analysis_indicator(code)
        if result2:
            for key, value in result2.items():
                if key not in all_results["stock_financial_analysis_indicator"]:
                    all_results["stock_financial_analysis_indicator"][key] = []
                all_results["stock_financial_analysis_indicator"][key].append(f"{code}: {value}")
        
        time.sleep(1)  # APIレート制限対策
        
        # テスト3: stock_zh_a_spot_em（最初の1回だけ実行）
        if code == TEST_STOCKS[0]:
            result3 = test_stock_zh_a_spot_em(code)
            if result3:
                for key, value in result3.items():
                    if key not in all_results["stock_zh_a_spot_em"]:
                        all_results["stock_zh_a_spot_em"][key] = []
                    all_results["stock_zh_a_spot_em"][key].append(f"{code}: {value}")
        
        time.sleep(1)  # APIレート制限対策
        
        # テスト4: その他のAPI（最初の1回だけ実行）
        if code == TEST_STOCKS[0]:
            result4 = test_other_apis(code)
            if result4:
                for key, info in result4.items():
                    if key not in all_results["other_apis"]:
                        all_results["other_apis"][key] = []
                    # infoの形式が複数あるので安全に整形
                    if isinstance(info, dict) and "value" in info:
                        all_results["other_apis"][key].append(f"{code}: {info.get('value')} (source: {info.get('source')})")
                    else:
                        all_results["other_apis"][key].append(f"{code}: {info}")
    
    # 結果サマリー
    print(f"\n\n{'='*60}")
    print("【結果サマリー】")
    print(f"{'='*60}")
    
    target_items = ["営業利益率", "ROA", "ROE", "配当利回り", "EPS"]
    
    for item in target_items:
        print(f"\n{item}:")
        found = False
        for api_name, results in all_results.items():
            if item in results:
                print(f"  ✅ {api_name}: 取得可能")
                found = True
            # 計算で取得できる場合も表示
            if item == "配当利回り" and "配当利回り(計算:推定)" in results:
                print(f"  ✅ {api_name}: 計算で取得可能")
                found = True
        if not found:
            print(f"  ❌ どのAPIからも取得できませんでした")
    
    print(f"\n{'='*60}")
    print("テスト完了")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
