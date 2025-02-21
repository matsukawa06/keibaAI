from urllib.request import urlopen, Request
import pandas as pd
from bs4 import BeautifulSoup
import re
import time #sleep用
from tqdm.notebook import tqdm

#引数の「: str」は型アノテーションで、呼び出し元から型が見える
def scrape_kaisai_date(from_: str, to_: str):
    """
    from_とto_をyyyy-mm形式で指定すると、間の開催日一覧を取得する関数
    """
    kaisai_data_list = []   #配列の初期化はforの外へ
    
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
            kaisai_data = re.findall(r"kaisai_date=(\d{8})", a["href"])[0]
            kaisai_data_list.append(kaisai_data)
    
    return kaisai_data_list
