import json
import random
import re
from pathlib import Path

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from tqdm.notebook import tqdm
from webdriver_manager.chrome import ChromeDriverManager

# commonディレクトリのパス
COMMON_DATA_DIR = Path("..", "..", "common", "data")
POPULATION_DIR = COMMON_DATA_DIR / "prediction_population"
MAPPING_DIR = COMMON_DATA_DIR / "mapping"
# v3_0_0ディレクトリのパス
DATA_DIR = Path("..", "data")
INPUT_DIR = DATA_DIR / "01_preprocessed"
OUTPUT_DIR = DATA_DIR / "02_features"
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

# カテゴリ変数を数値に変換するためのマッピング
with open(MAPPING_DIR / "sex.json", "r") as f:
    sex_mapping = json.load(f)
with open(MAPPING_DIR / "race_type.json", "r") as f:
    race_type_mapping = json.load(f)
with open(MAPPING_DIR / "around.json", "r") as f:
    around_mapping = json.load(f)
with open(MAPPING_DIR / "weather.json", "r") as f:
    weather_mapping = json.load(f)
with open(MAPPING_DIR / "ground_state.json", "r") as f:
    ground_state_mapping = json.load(f)
with open(MAPPING_DIR / "race_class.json", "r") as f:
    race_class_mapping = json.load(f)

# 2024/11/18追記：netkeiba.comの仕様変更により、スクレイピング時にuser agentの設定が必要になったため、
# 以下からランダムに抽出
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:115.0) Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Edg/115.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 OPR/85.0.4341.72",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 OPR/85.0.4341.72",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Vivaldi/5.3.2679.55",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Vivaldi/5.3.2679.55",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Brave/1.40.107",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Brave/1.40.107",
]


class PredictionFeatureCreator:
    def __init__(
        self,
        population_dir: Path = POPULATION_DIR,
        population_filename: str = "population.csv",
        input_dir: Path = INPUT_DIR,
        horse_results_filename: Path = "horse_results_prediction.csv",
        jockey_leading_filename: Path = "jockey_leading.csv",
        trainer_leading_filename: Path = "trainer_leading.csv",
        peds_filename: str = "peds_prediction.csv",
        sire_leading_filename: str = "sire_leading.csv",
        output_dir: Path = OUTPUT_DIR,
        output_filename: str = "features_prediction.csv",
    ):
        self.population = pd.read_csv(population_dir / population_filename, sep="\t")
        self.horse_results = pd.read_csv(input_dir / horse_results_filename, sep="\t")
        self.jockey_leading = pd.read_csv(input_dir / jockey_leading_filename, sep="\t")
        self.trainer_leading = pd.read_csv(
            input_dir / trainer_leading_filename, sep="\t"
        )
        self.peds = pd.read_csv(input_dir / peds_filename, sep="\t")
        self.sire_leading = pd.read_csv(input_dir / sire_leading_filename, sep="\t")
        self.output_dir = output_dir
        self.output_filename = output_filename
        self.htmls = {}
        self.agg_horse_per_group_cols_dfs = {}

    def create_baselog(self):
        """
        horse_resultsをレース結果テーブルの日付よりも過去に絞り、集計元のログを作成。
        """
        self.baselog = (
            self.population.merge(
                self.horse_results, on="horse_id", suffixes=("", "_horse")
            )
            .query("date_horse < date")
            .sort_values("date_horse", ascending=False)
        )

    def agg_horse_n_races(self, n_races: list[int] = [3, 5, 10, 1000]) -> None:
        """
        直近nレースの着順と賞金の平均を集計する関数。
        出走馬が確定した時点で先に実行しておいても良い。
        """
        grouped_df = self.baselog.groupby(["race_id", "horse_id"])
        merged_df = self.population.copy()
        for n_race in n_races:
            df = (
                grouped_df.head(n_race)
                .groupby(["race_id", "horse_id"])[["rank", "prize"]]
                .mean()
                .add_suffix(f"_{n_race}races")
            )
            merged_df = merged_df.merge(df, on=["race_id", "horse_id"])
        self.agg_horse_n_races_df = merged_df

    def agg_horse_n_races_relative(
        self, n_races: list[int] = [2, 3, 5, 10, 1000]
    ) -> None:
        """
        直近nレースの着順と賞金の平均を集計する関数。
        """
        grouped_df = self.baselog.groupby(["race_id", "horse_id"])
        merged_df = self.population.copy()
        for n_race in tqdm(n_races, desc="agg_horse_n_races_relative"):
            df = (
                grouped_df.head(n_race)
                .groupby(["race_id", "horse_id"])[
                    [
                        "rank",
                        "prize",
                        "rank_diff",
                        "race_class",
                    ]
                ]
                .agg(["mean", "median", "max", "min"])
            )
            df.columns = ["_".join(col) + f"_{n_race}races" for col in df.columns]
            # レースごとの相対値に変換
            tmp_df = df.groupby(["race_id"])
            relative_df = ((df - tmp_df.mean()) / tmp_df.std()).add_suffix("_relative")
            merged_df = merged_df.merge(
                relative_df, on=["race_id", "horse_id"], how="left"
            )
        self.agg_horse_n_races_relative_df = merged_df

    def agg_interval(self):
        """
        前走からの出走間隔を集計する関数。
        """
        print("running agg_interval()...")
        df = (
            self.baselog.groupby(["race_id", "horse_id", "date"])["date_horse"]
            .max()
            .reset_index()
        )
        df["interval"] = (
            pd.to_datetime(df["date"]) - pd.to_datetime(df["date_horse"])
        ).dt.days
        self.agg_interval_df = df

    def fetch_shubuta_page_html(self, race_id: str) -> None:
        """
        レースidを指定すると、出馬表ページのhtmlをスクレイピングする関数。
        """
        print("fetching shubuta page html...")
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--user-agent=" + random.choice(USER_AGENTS))
        # chrome driverをインストール
        driver_path = ChromeDriverManager().install()
        url = f"https://race.netkeiba.com/race/shutuba.html?race_id={race_id}"
        with webdriver.Chrome(service=Service(driver_path), options=options) as driver:
            driver.implicitly_wait(10)
            driver.get(url)
            self.htmls[race_id] = driver.page_source

    def fetch_results(
        self, race_id: str, html: str, sex_mapping: dict = sex_mapping
    ) -> None:
        """
        出馬表ページのhtmlを受け取ると、
        「レース結果テーブル」を取得して、学習時と同じ形式に前処理する関数。
        """
        df = pd.read_html(html)[0]
        df.columns = df.columns.get_level_values(1)
        soup = BeautifulSoup(html, "lxml").find("table", class_="Shutuba_Table")
        horse_id_list = []
        a_list = soup.find_all("a", href=re.compile(r"/horse/"))
        for a in a_list:
            horse_id = re.findall(r"\d{10}", a["href"])[0]
            horse_id_list.append(int(horse_id))
        df["horse_id"] = horse_id_list
        jockey_id_list = []
        a_list = soup.find_all("a", href=re.compile(r"/jockey/"))
        for a in a_list:
            jockey_id = re.findall(r"\d{5}", a["href"])[0]
            jockey_id_list.append(int(jockey_id))
        df["jockey_id"] = jockey_id_list
        trainer_id_list = []
        a_list = soup.find_all("a", href=re.compile(r"/trainer/"))
        for a in a_list:
            trainer_id = re.findall(r"\d{5}", a["href"])[0]
            trainer_id_list.append(int(trainer_id))
        df["trainer_id"] = trainer_id_list
        # 前処理
        df = df[df.iloc[:, 2] != "取消"]
        df["wakuban"] = df.iloc[:, 0].astype(int)
        df["umaban"] = df.iloc[:, 1].astype(int)
        df["sex"] = df.iloc[:, 4].str[0].map(sex_mapping)
        df["age"] = df.iloc[:, 4].str[1:].astype(int)
        df["impost"] = df.iloc[:, 5].astype(float)
        df["weight"] = df.iloc[:, 8].str.extract(r"(\d+)").astype(int)
        df["weight_diff"] = (
            df.iloc[:, 8]
            .str.replace("前計不", "0", regex=False)
            .str.extract(r"\((.+)\)")
            .fillna(0)
            .astype(int)
        )
        df["tansho_odds"] = df.iloc[:, 9].astype(float)
        df["popularity"] = df.iloc[:, 10].astype(int)
        df["race_id"] = int(race_id)
        df["n_horses"] = df.groupby("race_id")["race_id"].transform("count")
        # 使用する列を選択
        df = df[
            [
                "race_id",
                "horse_id",
                "jockey_id",
                "trainer_id",
                "umaban",
                "wakuban",
                "tansho_odds",
                "popularity",
                "impost",
                "sex",
                "age",
                "weight",
                "weight_diff",
                "n_horses",
            ]
        ]
        self.results = df

    def fetch_race_info(
        self,
        race_id: str,
        html: str,
        race_type_mapping: dict = race_type_mapping,
        around_mapping: dict = around_mapping,
        weather_mapping: dict = weather_mapping,
        ground_state_mapping: dict = ground_state_mapping,
        race_class_mapping: dict = race_class_mapping,
    ):
        """
        出馬表ページのhtmlを受け取ると、
        「レース情報テーブル」を取得して、学習時と同じ形式に前処理する関数。
        """
        info_dict = {}
        info_dict["race_id"] = int(race_id)
        soup = BeautifulSoup(html, "lxml").find("div", class_="RaceList_Item02")
        title = soup.find("h1").text.strip()
        divs = soup.find_all("div")
        div1 = divs[0].text.replace(" ", "")
        info1 = re.findall(r"[\w:]+", div1)
        info_dict["race_type"] = race_type_mapping[info1[1][0]]
        info_dict["around"] = (
            around_mapping[info1[2][0]] if info_dict["race_type"] != 2 else np.nan
        )
        info_dict["course_len"] = int(re.findall(r"\d+", info1[1])[0])
        info_dict["weather"] = weather_mapping[re.findall(r"天候:(\w+)", div1)[0]]
        info_dict["ground_state"] = ground_state_mapping[
            re.findall(r"馬場:(\w+)", div1)[0]
        ]
        # レース階級情報の取得
        regex_race_class = "|".join(race_class_mapping)
        race_class_title = re.findall(regex_race_class, title)
        # タイトルからレース階級情報が取れない場合
        race_class = re.findall(regex_race_class, divs[1].text)
        if len(race_class_title) != 0:
            info_dict["race_class"] = race_class_mapping[race_class_title[0]]
        elif len(race_class) != 0:
            info_dict["race_class"] = race_class_mapping[race_class[0]]
        else:
            info_dict["race_class"] = None
        info_dict["place"] = int(race_id[4:6])
        self.race_info = pd.DataFrame(info_dict, index=[0])

    def agg_horse_per_course_len(
        self, n_races: list[int] = [1, 2, 3, 5, 10, 20]
    ) -> None:
        """
        直近nレースの馬の過去成績を距離・race_typeごとに集計し、相対値に変換する関数。
        """
        baselog = (
            self.population.merge(
                self.race_info[["race_id", "course_len", "race_type"]], on="race_id"
            )
            .merge(
                self.horse_results,
                on=["horse_id", "course_len", "race_type"],
                suffixes=("", "_horse"),
            )
            .query("date_horse < date")
            .sort_values("date_horse", ascending=False)
        )
        grouped_df = baselog.groupby(["race_id", "horse_id"])
        merged_df = self.population.copy()
        for n_race in tqdm(n_races, desc="agg_horse_per_course_len"):
            df = (
                grouped_df.head(n_race)
                .groupby(["race_id", "horse_id"])[
                    [
                        "rank",
                        "prize",
                        "rank_diff",
                        "time",
                        "win",
                        "show",
                    ]
                ]
                .agg(["mean", "min"])
            )
            df.columns = [
                "_".join(col) + f"_{n_race}races_per_course_len" for col in df.columns
            ]
            # レースごとの相対値に変換
            tmp_df = df.groupby(["race_id"])
            relative_df = ((df - tmp_df.mean()) / tmp_df.std()).add_suffix("_relative")
            merged_df = merged_df.merge(
                relative_df, on=["race_id", "horse_id"], how="left"
            )
        self.agg_horse_per_course_len_df = merged_df

    def agg_horse_per_group_cols(
        self,
        group_cols: list[str],
        df_name: str,
        n_races: list[int] = [1, 2, 3, 5, 10, 20],
    ) -> None:
        """
        直近nレースの馬の過去成績をgroup_colsごとに集計し、相対値に変換する関数。
        """
        baselog = (
            self.population.merge(
                self.race_info[["race_id"] + group_cols], on="race_id"
            )
            .merge(
                self.horse_results,
                on=["horse_id"] + group_cols,
                suffixes=("", "_horse"),
            )
            .query("date_horse < date")
            .sort_values("date_horse", ascending=False)
        )
        grouped_df = baselog.groupby(["race_id", "horse_id"])
        merged_df = self.population.copy()
        for n_race in tqdm(n_races, desc=f"agg_horse_per_{df_name}"):
            df = (
                grouped_df.head(n_race)
                .groupby(["race_id", "horse_id"])[
                    [
                        "rank",
                        "prize",
                        "rank_diff",
                        "time",
                        "win",
                        "show",
                    ]
                ]
                .agg(["mean", "max", "min"])
            )
            df.columns = [
                "_".join(col) + f"_{n_race}races_per_{df_name}" for col in df.columns
            ]
            # レースごとの相対値に変換
            tmp_df = df.groupby(["race_id"])
            relative_df = ((df - tmp_df.mean()) / tmp_df.std()).add_suffix("_relative")
            merged_df = merged_df.merge(
                relative_df, on=["race_id", "horse_id"], how="left"
            )
        self.agg_horse_per_group_cols_dfs[df_name] = merged_df

    def agg_jockey(self):
        """
        騎手の過去成績を紐付け、相対値に変換する関数。
        """
        print("running agg_jockey()...")
        df = self.population.merge(
            self.results[["race_id", "horse_id", "jockey_id"]],
            on=["race_id", "horse_id"],
        )
        df["year"] = pd.to_datetime(df["date"]).dt.year - 1
        df = (
            df.merge(self.jockey_leading, on=["jockey_id", "year"], how="left")
            .drop(["date", "jockey_id", "year"], axis=1)
            .set_index(["race_id", "horse_id"])
            .add_prefix("jockey_")
        )
        # レースごとの相対値に変換
        tmp_df = df.groupby(["race_id"])
        relative_df = ((df - tmp_df.mean()) / tmp_df.std()).add_suffix("_relative")
        self.agg_jockey_df = relative_df

    def agg_trainer(self):
        """
        調教師の過去成績を紐付け、相対値に変換する関数。
        """
        print("running agg_trainer()...")
        df = self.population.merge(
            self.results[["race_id", "horse_id", "trainer_id"]],
            on=["race_id", "horse_id"],
        )
        df["year"] = pd.to_datetime(df["date"]).dt.year - 1
        df = (
            df.merge(self.trainer_leading, on=["trainer_id", "year"], how="left")
            .drop(["date", "trainer_id", "year"], axis=1)
            .set_index(["race_id", "horse_id"])
            .add_prefix("trainer_")
        )
        # レースごとの相対値に変換
        tmp_df = df.groupby(["race_id"])
        relative_df = ((df - tmp_df.mean()) / tmp_df.std()).add_suffix("_relative")
        self.agg_trainer_df = relative_df

    def agg_sire(self):
        """
        種牡馬の過去成績を紐付け、相対値に変換する関数。
        """
        print("running agg_sire()...")
        df = self.population.merge(
            self.peds[["horse_id", "sire_id"]],
            on="horse_id",
        ).merge(
            self.race_info[["race_id", "race_type", "course_len"]],
        )
        df["year"] = pd.to_datetime(df["date"]).dt.year - 1
        df = df.merge(
            self.sire_leading,
            on=["sire_id", "year", "race_type"],
            suffixes=("", "_sire"),
        ).set_index(["race_id", "horse_id"])
        df["course_len_diff"] = df["course_len"] - df["course_len_sire"]
        df = df[["n_races", "n_wins", "winrate", "course_len_diff"]].add_prefix("sire_")
        # レースごとの相対値に変換
        tmp_df = df.groupby(["race_id"])
        relative_df = ((df - tmp_df.mean()) / tmp_df.std()).add_suffix("_relative")
        self.agg_sire_df = relative_df

    def cross_features(self):
        """
        交互作用特徴量を作成する関数。
        """
        print("running cross_feature()...")
        df = self.population.merge(
            self.race_info[["race_id", "race_type", "around"]],
            on="race_id",
        ).merge(
            self.results[["race_id", "horse_id", "wakuban", "umaban", "sex"]],
            on=["race_id", "horse_id"],
        )
        df["date"] = pd.to_datetime(df["date"])
        df["month"] = df["date"].dt.month
        df["sin_date"] = np.sin(2 * np.pi * df["date"].dt.dayofyear / 365.25) + 1
        df["cos_date"] = np.cos(2 * np.pi * df["date"].dt.dayofyear / 365.25) + 1
        df["wakuban_race_type"] = df["race_type"].map({0: 1, 1: -1}) * df["wakuban"]
        df["umaban_race_type"] = df["race_type"].map({0: 1, 1: -1}) * df["umaban"]
        df["wakuban_around"] = df["around"].map({2: 1}) * df["wakuban"]
        df["umaban_around"] = df["around"].map({2: 1}) * df["umaban"]
        df["month_sex"] = df["sex"].map({1: 1, 0: -1}) * df["month"]
        df["sin_date_sex"] = df["sex"].map({1: 1, 0: -1}) * df["sin_date"]
        df["cos_date_sex"] = df["sex"].map({1: 1, 0: -1}) * df["cos_date"]
        self.cross_features_df = df[
            [
                "race_id",
                "horse_id",
                "wakuban_race_type",
                "umaban_race_type",
                "wakuban_around",
                "umaban_around",
                "month",
                "sin_date",
                "cos_date",
                "month_sex",
                "sin_date_sex",
                "cos_date_sex",
            ]
        ]

    def create_features(
        self, race_id: str, skip_agg_horse: bool = False
    ) -> pd.DataFrame:
        """
        特徴量作成処理を実行し、populationテーブルに全ての特徴量を結合する。
        先に馬の過去成績集計を実行しておいた場合は、
        skip_agg_horse=Trueとすればスキップできる。
        """
        # 馬の過去成績集計（先に実行しておいた場合は、スキップできる）
        if not skip_agg_horse:
            self.create_baselog()
            self.agg_horse_n_races()
            self.agg_horse_n_races_relative()
            self.agg_interval()
        # 各種テーブルの取得
        self.fetch_shubuta_page_html(race_id)
        self.fetch_results(race_id, self.htmls[race_id])
        self.fetch_race_info(race_id, self.htmls[race_id])
        # グループごとの馬の過去成績集計（race_infoのカラムが必要なため、ここで実行）
        self.agg_horse_per_course_len()
        self.agg_horse_per_group_cols(
            group_cols=["ground_state", "race_type"], df_name="ground_state_race_type"
        )
        self.agg_horse_per_group_cols(group_cols=["race_class"], df_name="race_class")
        self.agg_horse_per_group_cols(group_cols=["race_type"], df_name="race_type")
        # リーディングデータの紐付け
        self.agg_jockey()
        self.agg_trainer()
        self.agg_sire()
        self.cross_features()
        # 全ての特徴量を結合
        print("merging all features...")
        features = (
            self.population.merge(self.results, on=["race_id", "horse_id"])
            .merge(self.race_info, on="race_id")
            .merge(
                self.agg_horse_n_races_df,
                on=["race_id", "horse_id"],
                how="left",
            )
            .merge(
                self.agg_horse_n_races_relative_df,
                on=["race_id", "horse_id"],
                how="left",
            )
            .merge(
                self.agg_jockey_df,
                on=["race_id", "horse_id"],
                how="left",
            )
            .merge(
                self.agg_trainer_df,
                on=["race_id", "horse_id"],
                how="left",
            )
            .merge(
                self.agg_horse_per_course_len_df,
                on=["race_id", "date", "horse_id"],
                how="left",
            )
            .merge(
                self.agg_horse_per_group_cols_dfs["ground_state_race_type"],
                on=["race_id", "date", "horse_id"],
                how="left",
            )
            .merge(
                self.agg_horse_per_group_cols_dfs["race_class"],
                on=["race_id", "date", "horse_id"],
                how="left",
            )
            .merge(
                self.agg_horse_per_group_cols_dfs["race_type"],
                on=["race_id", "date", "horse_id"],
                how="left",
            )
            .merge(
                self.agg_sire_df,
                on=["race_id", "horse_id"],
                how="left",
            )
            .merge(
                self.agg_interval_df,
                on=["race_id", "date", "horse_id"],
                how="left",
            )
            .merge(
                self.cross_features_df,
                on=["race_id", "horse_id"],
                how="left",
            )
        )
        features.to_csv(self.output_dir / self.output_filename, sep="\t", index=False)
        return features
