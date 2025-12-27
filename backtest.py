import akshare as ak
import pandas as pd
from tqdm import tqdm
from datetime import datetime, timedelta
import os
import time
import random

# ============================
# パラメータ
# ============================
DATA_DIR = "data/daily"
OUTPUT_DIR = "output"

MA_WINDOW = 20
VOL_MULTIPLE = 2.0
MIN_PCT_CHG = 3.0
MIN_AMOUNT = 1e8

TODAY = datetime.today().strftime("%Y%m%d")
DEFAULT_START = "20180101"   # 初回DL用

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

Z_THRESHOLD = 2.5
STD_FLOOR = 1e-6   # 分散ゼロ対策

def safe_call(func, retries=5, sleep=3, **kwargs):
    for i in range(retries):
        try:
            return func(**kwargs)
        except Exception as e:
            if i == retries - 1:
                raise
            time.sleep(sleep + random.random())

# ============================
# 上海A株一覧（安定API）
# ============================
STOCK_LIST_PATH = "data/stock_list_sh.csv"

if os.path.exists(STOCK_LIST_PATH):
    stocks = pd.read_csv(STOCK_LIST_PATH)
else:
    stocks = safe_call(ak.stock_sh_a_spot_em)
    stocks = stocks[stocks["代码"].str.startswith("60")]
    stocks.to_csv(STOCK_LIST_PATH, index=False, encoding="utf-8-sig")

# ============================
# 補助関数
# ============================
def load_or_download(code: str) -> pd.DataFrame:
    """
    既存CSVがあれば不足分だけDLして追記
    """
    path = f"{DATA_DIR}/{code}.csv"

    if os.path.exists(path):
        df_old = pd.read_csv(path)
        last_date = df_old["日期"].max()
        start_date = (pd.to_datetime(last_date) + pd.Timedelta(days=1)).strftime("%Y%m%d")
    else:
        df_old = None
        start_date = DEFAULT_START

    if start_date > TODAY:
        return df_old

    try:
        df_new = ak.stock_zh_a_hist(
            symbol=code,
            period="daily",
            start_date=start_date,
            end_date=TODAY,
            adjust="qfq"
        )
    except Exception as e:
        print(f"[ERROR] {code}: {e}")
        return df_old

    if df_new is None or df_new.empty:
        return df_old

    if df_old is not None:
        df = pd.concat([df_old, df_new], ignore_index=True)
        df = df.drop_duplicates(subset=["日期"]).sort_values("日期")
    else:
        df = df_new.sort_values("日期")

    df.to_csv(path, index=False, encoding="utf-8-sig")
    return df

# ============================
# 出来高急増検知（倍率＋Z-score 両対応）
# ============================
def detect_volume_spike(df):
    """
    最新日の倍率法・Z-score法の判定結果を返す
    """
    df = df.sort_values("日期").copy()

    df["vol_mean"] = df["成交量"].rolling(MA_WINDOW).mean()
    df["vol_std"]  = df["成交量"].rolling(MA_WINDOW).std().clip(lower=STD_FLOOR)
    df["z_score"]  = (df["成交量"] - df["vol_mean"]) / df["vol_std"]

    latest = df.iloc[-1]

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

def detect_volume_spike_at(df, idx):
    """
    df.iloc[idx] をシグナル日として判定
    """
    if idx < MA_WINDOW:
        return False, False, None

    window = df.iloc[idx - MA_WINDOW:idx]

    mean = window["成交量"].mean()
    std  = window["成交量"].std()
    std = max(std, 1e-6)

    today = df.iloc[idx]

    vol_ratio = today["成交量"] / mean
    z_score = (today["成交量"] - mean) / std

    common_filter = (
        today["涨跌幅"] >= MIN_PCT_CHG
        and today["涨跌幅"] < 9.5
        and today["成交额"] >= MIN_AMOUNT
    )

    ratio_hit = vol_ratio >= VOL_MULTIPLE and common_filter
    z_hit = z_score >= Z_THRESHOLD and common_filter

    return ratio_hit, z_hit, {
        "date": today["日期"],
        "vol_ratio": vol_ratio,
        "z_score": z_score,
        "close": today["收盘"],
    }

# ============================
# リターン計算関数
# ============================
def calc_forward_return(df, days):
    """
    最新日からdays後の終値リターン
    """
    df = df.sort_values("日期")
    print(df)
    if len(df) < days + 1:
        return None

    entry = df.iloc[-1]["收盘"]
    future = df.iloc[-1 + days]["收盘"]

    return (future - entry) / entry

# ============================
# 結果集計
# ============================
def summarize(bt):
    df = pd.DataFrame(bt, columns=["ret_1d", "ret_5d"]).dropna()
    return {
        "count": len(df),
        "avg_1d": df["ret_1d"].mean(),
        "win_1d": (df["ret_1d"] > 0).mean(),
        "avg_5d": df["ret_5d"].mean(),
        "win_5d": (df["ret_5d"] > 0).mean(),
    }

# ============================
# バックテスト実装
# ============================
bt_ratio = []
bt_z = []

#for _, row in tqdm(stocks.iterrows(), total=len(stocks)):
for _, row in stocks.iterrows():
    time.sleep(0.3)  # ← 重要
    code = row["代码"]

    try:
        df = load_or_download(code)
        if df is None or len(df) < MA_WINDOW + 6:
            continue

        df = df.sort_values("日期").reset_index(drop=True)
        # 最後の5日は未来が見えないので除外
        for i in range(MA_WINDOW, len(df) - 5):

            ratio_hit, z_hit, info = detect_volume_spike_at(df, i)
            if info is None:
                continue

            entry = info["close"]
            ret_1d = (df.iloc[i + 1]["收盘"] - entry) / entry
            ret_5d = (df.iloc[i + 5]["收盘"] - entry) / entry

            if ratio_hit:
                bt_ratio.append([ret_1d, ret_5d])
                # print(f"ratio {code}: {ret_1d}, {ret_5d}")

            if z_hit:
                bt_z.append([ret_1d, ret_5d])
                # print(f"z {code}: {ret_1d}, {ret_5d}")

    except Exception:
        continue

summary = pd.DataFrame.from_dict({
    "ratio_method": summarize(bt_ratio),
    "z_score_method": summarize(bt_z),
}, orient="index")

print(summary)
