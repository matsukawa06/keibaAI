import random
import re
import time
from pathlib import Path
from urllib.request import urlopen, Request

import pandas as pd
import scraping
from bs4 import BeautifulSoup
from tqdm.notebook import tqdm

POPULATION_DIR = Path("..", "data", "prediction_population")
POPULATION_DIR.mkdir(exist_ok=True, parents=True)

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

def scrape_horse_id_list(race_id: str) -> list[str]:
    """
    レースidを指定すると、出走馬id一覧が返ってくる関数。
    """
    url = f"https://race.netkeiba.com/race/shutuba.html?race_id={race_id}"
    request = Request(url, headers={"User-Agent": random.choice(USER_AGENTS)})
    html = urlopen(request).read()  # スクレイピング)
    soup = BeautifulSoup(html, "lxml")
    td_list = soup.find_all("td", class_="HorseInfo")
    horse_id_list = []
    for td in td_list:
        horse_id = re.findall(r"\d{10}", td.find("a")["href"])[0]
        horse_id_list.append(horse_id)
    return horse_id_list

def create(
        kaisai_date: str,
        save_dir: Path = POPULATION_DIR,
        save_filename: str = "population.csv",
) -> pd.DataFrame:
    """
    開催日（yyyymmdd形式）を指定すると、予測母集団である
    (date, race_id, horse_id)のDataFrameが返ってくる関数。
    """
    print("scraping race_id_list...")
    race_id_list = scraping.scrape_race_id_list([kaisai_date])
    dfs = {}
    print("scraping horse_id_list...")
    for race_id in tqdm(race_id_list):
        horse_id_list = scrape_horse_id_list(race_id)
        time.sleep(1)
        df = pd.DataFrame(
            {"date": kaisai_date, "race_id": race_id, "horse_id": horse_id_list}
        )
        dfs[race_id] = df
    concat_df = pd.concat(dfs.values())
    concat_df["date"] = pd.to_datetime(concat_df["date"])
    concat_df.to_csv(save_dir / save_filename, index=False, sep="\t")
    return concat_df