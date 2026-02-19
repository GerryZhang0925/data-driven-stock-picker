"""
設定と定数
"""
from datetime import datetime
import os

# ディレクトリ設定
DATA_DIR = "data/daily"  # 後方互換性のため残す（非推奨）
DATA_DIR_CN = "data/daily/cn"  # 中国株用データディレクトリ
DATA_DIR_JP = "data/daily/jp"  # 日本株用データディレクトリ
OUTPUT_DIR = "output"

# 分析パラメータ
MA_WINDOW = 20
VOL_MULTIPLE = 2.0
MIN_PCT_CHG = 3.0
MIN_AMOUNT = 1e8
Z_THRESHOLD = 2.5
STD_FLOOR = 1e-6   # 分散ゼロ対策

# 日付設定
TODAY = datetime.today().strftime("%Y%m%d")
DEFAULT_START = "20180101"   # 初回DL用

# ディレクトリ作成
os.makedirs(DATA_DIR, exist_ok=True)  # 後方互換性のため
os.makedirs(DATA_DIR_CN, exist_ok=True)
os.makedirs(DATA_DIR_JP, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ファイルパス
STOCK_LIST_PATH = f"{DATA_DIR_CN}/stock_list_sh_xsb.csv"  # 中国株用銘柄リスト

