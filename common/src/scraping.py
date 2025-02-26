from urllib.request import urlopen, Request
import pandas as pd
from bs4 import BeautifulSoup
import re
import time #sleep用
from tqdm.notebook import tqdm
from selenium.webdriver.chrome.options import Options
from tqdm.notebook import tqdm
import traceback
from selenium.webdriver.common.by import By
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from pathlib import Path
import random

# 定数は大文字で指定する
HTML_DIR = Path("..", "data", "html")
HTML_RACE_DIR = HTML_DIR / "race"
HTML_HORSE_DIR = HTML_DIR / "horse"

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

#引数の「: str」は型アノテーションで、呼び出し元から型が見える
def scrape_kaisai_date(from_: str, to_: str) -> list[str]:
    """
    from_とto_をyyyy-mm形式で指定すると、間の開催日一覧を取得する関数。
    """
    kaisai_date_list = []   #配列の初期化はforの外へ
    
    #tqdmをfor文のin のあとにつける
    for date in tqdm(pd.date_range(from_, to_, freq="MS")):
        year = date.year
        month = date.month
        url = f"https://race.netkeiba.com/top/calendar.html?year={year}&month={month}"
        headers = {"User-Agent": "Mozilla/5.0"}
        request = Request(url, headers=headers)
        html = urlopen(request).read()  #スクレイピング
        #forでスクレイピングする場合サイトに負荷をかけないように必ずsleepを入れる！！
        time.sleep(1)   # 1秒スリープ
        #htmlを加工
        soup = BeautifulSoup(html, "lxml")
        a_list = soup.find("table", class_="Calendar_Table").find_all("a") 
        for a in a_list:
            kaisai_date = re.findall(r"kaisai_date=(\d{8})", a["href"])[0]
            kaisai_date_list.append(kaisai_date)
    
    return kaisai_date_list

def scrape_race_id_list(kaisai_date_list: list[str]) -> list[str]:
    '''
    開催日（yyyymmdd形式）をリストで入れると、レースid一覧が返ってくる関数。
    '''
    driver_path = ChromeDriverManager().install()
    options = Options()
    options.add_argument("--user-agent=Mozilla/5.0")
    options.add_argument("--headless")
    race_id_list = []
    with webdriver.Chrome(service=Service(driver_path), options=options) as driver:
        for kaisai_date in tqdm(kaisai_date_list):
            url = f"https://race.netkeiba.com/top/race_list.html?kaisai_date={kaisai_date}"
            try:
                driver.set_window_size(50, 50)  # 画面サイズをなるべく小さくして、余計な画像などを読み込まないようにする
                driver.get(url)
                time.sleep(1)
                li_list = driver.find_elements(By.CLASS_NAME, "RaceList_DataItem")
                for li in li_list:
                    href = li.find_element(By.TAG_NAME, "a").get_attribute("href")
                    race_id = re.findall(r"race_id=(\d{12})", href)[0]
                    race_id_list.append(race_id)
            except:
                print(f"stopped at {url}")
                # エラーの内容を表示する
                print(traceback.format_exc())
                # 処理に失敗したとき、forをbreakすることで途中までの結果がreturnされる
                break
    return race_id_list

# 保存するdirは毎回変えるわけでもないので、初期化する
def scrape_html_race(race_id_list: list[str], save_dir: Path = HTML_RACE_DIR) -> list[Path]:
    '''
    netkeiba.comのraceページのhtmlをスクレイピングして、save_dirに保存する関数。
    すでにhtmlが存在する場合はスキップされて、新たに取得されたhtmlのパスだけが
    返ってくる。
    '''
    html_path_list = []
    # parents=Trueで、指定より上位のdirが存在しない場合、上位も作成する
    # exist_ok=Trueで、すでに存在する場合、同じものを作成してOK！これ指定しないと既に存在しますエラーになる
    save_dir.mkdir(parents=True, exist_ok=True)
    for race_id in tqdm(race_id_list):
        filepath = save_dir / f"{race_id}.bin"  # ファイルパスをスクレイピング前に移動
        # ファイルが既に存在する場合はスキップする
        if filepath.is_file():
            print(f"skipped: {race_id}")
        else:
            url = f"https://db.netkeiba.com/race/{race_id}"
            headers = {"User-Agent": random.choice(USER_AGENTS)}
            request = Request(url, headers=headers)
            html = urlopen(request).read()
            time.sleep(3)
            with open(filepath, "wb") as f:
                f.write(html)
            html_path_list.append(filepath)
    return html_path_list

def scrape_html_horse(
        horse_id_list: list[str],
        save_dir: Path = HTML_HORSE_DIR,
        skip: bool = True
    ) -> list[Path]:
    '''
    netkeiba.comのhorseページのhtmlをスクレイピングして、save_dirに保存する関数。
    すでにhtmlが存在、skip=True場合はスキップされて、新たに取得されたhtmlのパスだけが
    返ってくる。
    horseページは出走すれば情報が更新されていくため、スキップするかを引数に追加する。
    '''
    html_path_list = []
    # parents=Trueで、指定より上位のdirが存在しない場合、上位も作成する
    # exist_ok=Trueで、すでに存在する場合、同じものを作成してOK！これ指定しないと既に存在しますエラーになる
    save_dir.mkdir(parents=True, exist_ok=True)
    for horse_id in tqdm(horse_id_list):
        filepath = save_dir / f"{horse_id}.bin"  # ファイルパスをスクレイピング前に移動
        # ファイルが既に存在し、skip=True場合はスキップする
        if filepath.is_file() and skip:
            print(f"skipped: {horse_id}")
        else:
            url = f"https://db.netkeiba.com/horse/{horse_id}"
            headers = {"User-Agent": random.choice(USER_AGENTS)}
            request = Request(url, headers=headers)
            html = urlopen(request).read()
            time.sleep(3)
            with open(filepath, "wb") as f:
                f.write(html)
            html_path_list.append(filepath)
    return html_path_list