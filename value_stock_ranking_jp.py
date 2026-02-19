"""
日本株の収益バリュー株ランキング生成
スクリーニング条件はJSONファイルから読み込む
"""
import json
import os
import pandas as pd
from jp.fundamentals import load_fundamentals, FUNDAMENTALS_PATH_JP
from config import OUTPUT_DIR

# 設定ファイルのパス
CONFIG_FILE = "value_stock_screening_config.json"
OUTPUT_FILE = f"{OUTPUT_DIR}/value_stock_ranking_jp.csv"


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
            "market_cap_max": 30000000000
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
            "market_cap_max": 30000000000
        }


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
    
    # 営業利益率10％以上
    if '営業利益率' in filtered_df.columns:
        filtered_df = filtered_df[
            (filtered_df['営業利益率'].notna()) & 
            (filtered_df['営業利益率'] >= config.get('operating_margin_min', 10.0))
        ]
        print(f"営業利益率 {config.get('operating_margin_min', 10.0)}%以上: {len(filtered_df)}銘柄")
    
    # PER10倍以下
    if 'PER' in filtered_df.columns:
        filtered_df = filtered_df[
            (filtered_df['PER'].notna()) & 
            (filtered_df['PER'] > 0) & 
            (filtered_df['PER'] <= config.get('per_max', 10.0))
        ]
        print(f"PER {config.get('per_max', 10.0)}倍以下: {len(filtered_df)}銘柄")
    
    # PBR1.5倍以下
    if 'PBR' in filtered_df.columns:
        filtered_df = filtered_df[
            (filtered_df['PBR'].notna()) & 
            (filtered_df['PBR'] > 0) & 
            (filtered_df['PBR'] <= config.get('pbr_max', 1.5))
        ]
        print(f"PBR {config.get('pbr_max', 1.5)}倍以下: {len(filtered_df)}銘柄")
    
    # ROA7％以上
    if 'ROA' in filtered_df.columns:
        filtered_df = filtered_df[
            (filtered_df['ROA'].notna()) & 
            (filtered_df['ROA'] >= config.get('roa_min', 7.0))
        ]
        print(f"ROA {config.get('roa_min', 7.0)}%以上: {len(filtered_df)}銘柄")
    
    # 時価総額300億円以下
    if '時価総額' in filtered_df.columns:
        market_cap_max = config.get('market_cap_max', 30000000000)
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
    
    # ランキング用の列を選択
    ranking_columns = ['代码', '名称']
    if 'PER' in df.columns:
        ranking_columns.append('PER')
    if 'PBR' in df.columns:
        ranking_columns.append('PBR')
    if '営業利益率' in df.columns:
        ranking_columns.append('営業利益率')
    if 'ROA' in df.columns:
        ranking_columns.append('ROA')
    if 'ROE' in df.columns:
        ranking_columns.append('ROE')
    if '時価総額' in df.columns:
        ranking_columns.append('時価総額')
    if '配当利回り' in df.columns:
        ranking_columns.append('配当利回り')
    if 'EPS' in df.columns:
        ranking_columns.append('EPS')
    
    ranking_df = df[ranking_columns].copy()
    
    # 時価総額を億円単位に変換（表示用）
    if '時価総額' in ranking_df.columns:
        ranking_df['時価総額（億円）'] = (ranking_df['時価総額'] / 1e8).round(2)
    
    # 複合スコアを計算（PER、PBRが低く、営業利益率、ROAが高いほど良い）
    # スコア = (営業利益率 + ROA) / (PER + PBR) * 100
    if all(col in ranking_df.columns for col in ['営業利益率', 'ROA', 'PER', 'PBR']):
        ranking_df['バリュースコア'] = (
            (ranking_df['営業利益率'] + ranking_df['ROA']) / 
            (ranking_df['PER'] + ranking_df['PBR'] + 0.01) * 100
        ).round(2)
        # バリュースコアでソート
        ranking_df = ranking_df.sort_values('バリュースコア', ascending=False)
    else:
        # スコアが計算できない場合はPERでソート
        if 'PER' in ranking_df.columns:
            ranking_df = ranking_df.sort_values('PER')
    
    return ranking_df


def main():
    """メイン処理"""
    print("=== 日本株 収益バリュー株ランキング生成 ===\n")
    
    # 設定ファイルを読み込み
    config = load_screening_config()
    print(f"\nスクリーニング条件:")
    print(f"  営業利益率: {config.get('operating_margin_min', 10.0)}%以上")
    print(f"  PER: {config.get('per_max', 10.0)}倍以下")
    print(f"  PBR: {config.get('pbr_max', 1.5)}倍以下")
    print(f"  ROA: {config.get('roa_min', 7.0)}%以上")
    print(f"  時価総額: {config.get('market_cap_max', 30000000000) / 1e8:.0f}億円以下\n")
    
    # ファンダメンタル情報を読み込み
    print("ファンダメンタル情報を読み込み中...")
    fundamentals_df = load_fundamentals()
    
    if fundamentals_df is None:
        print("エラー: ファンダメンタル情報が見つかりません。")
        print("まず、get_fundamentals_jp.py を実行してファンダメンタル情報を取得してください。")
        return
    
    print(f"読み込み完了: {len(fundamentals_df)}銘柄\n")
    
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
