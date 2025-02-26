# keibaAI
# keibaAI
ディレクトリ構成
...
├─ common  ・・・バージョンにあまり関係なく共通的な処理
│    ├─ data
│    │   ├─ html
│    │   │   ├─ race
│    │   │   │   └─ {race_id}.bin   ・・・スクレイピングしたraceページのhtmle
│    │   │   └─ horse
│    │   │       └─ {horse_id}.bin  ・・・スクレイピングしたhorseページのhtml
│    │   └─ rawdf
│    │       ├─ resluts.csv
│    │       └─ horse_results.csv
│    ├─ requirements.txt
│    └─ src
│        ├─ create_rawdf.py  ・・・htmlをDataFrameに変換する関数を定義
│        ├─ dev.ipynb        ・・・開発用のnotebook
│        ├─ main.ipynb       ・・・コードを実行するnotebook
│        └─ scraping.py      ・・・スクレイピングする関数を定義
│
├─ v1_0_0  ・・・今後の開発はバージョンでわけて増やしていく