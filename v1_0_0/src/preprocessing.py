from pathlib import Path

import pandas as pd

COMMON_DATA_DIR = Path("..", "..", "common", "data")
INPUT_DIR = COMMON_DATA_DIR / "rawdf"
MAPPING_DIR = COMMON_DATA_DIR / "mapping"
OUTPUT_DIR = Path("..", "data", "01_preprocessed")
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

def porcess_results(
        input_dir: Path = INPUT_DIR,
        output_dir: Path = OUTPUT_DIR,
        save_filename: str = "results.csv",
        sex_mapping: dict = None,
) -> pd.DataFrame:
    '''
    レース結果テーブルのrawデータファイルをinput_dirから読み込んで、加工し、
    output_dirに保存する関数。
    '''
    df = pd.read_csv(input_dir / save_filename, sep="\t")
    df["rank"] = pd.to_numeric(df["着順"], errors="coerce")
    df.dropna(subset=["rank"], inplace=True)
    df["rank"] = df["rank"].astype(int)
    df["sex"] = df["性齢"].str[0].map(sex_mapping)
    df["age"] = df["性齢"].str[1:].astype(int)
    df["weight"] = df["馬体重"].str.extract(r"(\d+)").astype(int)
    df["weight"] = pd.to_numeric(df["weight"], errors="coerce")
    df["weight_diff"] = df["馬体重"].str.extract(r"\((.+)\)").astype(int)
    df["weight_diff"] = pd.to_numeric(df["weight_diff"], errors="coerce")
    df["tansho_odds"] = df["単勝"].astype(float)
    df["popularity"] = df["人気"].astype(int)
    df["impost"] = df["斤量"].astype(float)
    df["wakuban"] = df["枠番"].astype(int)
    df["umaban"] = df["馬番"].astype(int)
    # データが着順に並んでいることによるリーク防止のため、各レースを馬番順にソートする
    df = df.sort_values(["race_id", "umaban"])
    # 使用する列を選択
    df = df[
        [
            "race_id",
            "horse_id",
            "jockey_id",
            ""
        ]
        ]