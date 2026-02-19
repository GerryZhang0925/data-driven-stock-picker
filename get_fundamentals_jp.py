"""
日本株のファンダメンタル情報（PER、PBR、時価総額）を取得・保存
"""
from jp.fundamentals import update_fundamentals, load_fundamentals
import pandas as pd


def main():
    """メイン処理"""
    print("日本株のファンダメンタル情報を取得します...")
    
    # ファンダメンタル情報を更新
    fundamentals_df = update_fundamentals(force_update=False)
    
    if fundamentals_df is None:
        print("エラー: ファンダメンタル情報を取得できませんでした")
        return
    
    # 結果を表示
    print("\n=== ファンダメンタル情報サマリー ===")
    print(f"取得銘柄数: {len(fundamentals_df)}銘柄")
    
    # 数値列を数値型に変換（文字列が混在している場合に対応）
    numeric_columns = ['PER', 'PBR', '営業利益率', 'ROA', 'ROE', '時価総額', '配当利回り', 'EPS']
    for col in numeric_columns:
        if col in fundamentals_df.columns:
            # 文字列を数値に変換（エラー時はNaN）
            fundamentals_df[col] = pd.to_numeric(fundamentals_df[col], errors='coerce')
    
    # 表示用の列を決定（利用可能な列のみ）
    display_columns = ['代码', '名称']
    if 'PER' in fundamentals_df.columns:
        display_columns.append('PER')
    if 'PBR' in fundamentals_df.columns:
        display_columns.append('PBR')
    if '営業利益率' in fundamentals_df.columns:
        display_columns.append('営業利益率')
    if 'ROA' in fundamentals_df.columns:
        display_columns.append('ROA')
    if 'ROE' in fundamentals_df.columns:
        display_columns.append('ROE')
    if '時価総額' in fundamentals_df.columns:
        display_columns.append('時価総額')
    
    # PERでソートして上位10銘柄を表示
    if 'PER' in fundamentals_df.columns and fundamentals_df['PER'].notna().sum() > 0:
        print("\n【PER 低い順 上位10銘柄】")
        per_sorted = fundamentals_df[fundamentals_df['PER'].notna()].sort_values('PER').head(10)
        print(per_sorted[display_columns].to_string(index=False))
    
    # PBRでソートして上位10銘柄を表示
    if 'PBR' in fundamentals_df.columns and fundamentals_df['PBR'].notna().sum() > 0:
        print("\n【PBR 低い順 上位10銘柄】")
        pbr_sorted = fundamentals_df[fundamentals_df['PBR'].notna()].sort_values('PBR').head(10)
        print(pbr_sorted[display_columns].to_string(index=False))
    
    # 営業利益率でソートして上位10銘柄を表示
    if '営業利益率' in fundamentals_df.columns and fundamentals_df['営業利益率'].notna().sum() > 0:
        print("\n【営業利益率 高い順 上位10銘柄】")
        op_margin_sorted = fundamentals_df[fundamentals_df['営業利益率'].notna()].sort_values('営業利益率', ascending=False).head(10)
        print(op_margin_sorted[display_columns].to_string(index=False))
    
    # ROAでソートして上位10銘柄を表示
    if 'ROA' in fundamentals_df.columns and fundamentals_df['ROA'].notna().sum() > 0:
        print("\n【ROA 高い順 上位10銘柄】")
        roa_sorted = fundamentals_df[fundamentals_df['ROA'].notna()].sort_values('ROA', ascending=False).head(10)
        print(roa_sorted[display_columns].to_string(index=False))
    
    # 時価総額でソートして上位10銘柄を表示
    if '時価総額' in fundamentals_df.columns and fundamentals_df['時価総額'].notna().sum() > 0:
        print("\n【時価総額 高い順 上位10銘柄】")
        market_cap_sorted = fundamentals_df[fundamentals_df['時価総額'].notna()].sort_values('時価総額', ascending=False).head(10)
        # 時価総額を読みやすい形式に変換（億円単位）
        display_df = market_cap_sorted[display_columns].copy()
        if '時価総額' in display_df.columns:
            display_df['時価総額（億円）'] = (display_df['時価総額'] / 1e8).round(2)
            display_df = display_df.drop('時価総額', axis=1)
        print(display_df.to_string(index=False))
    
    from jp.fundamentals import FUNDAMENTALS_PATH_JP
    print(f"\n詳細データは {FUNDAMENTALS_PATH_JP} に保存されています")


if __name__ == "__main__":
    main()
