import elasticsearch
from elasticsearch import Elasticsearch, helpers
import os
import glob
import json

es = Elasticsearch("elasticsearch")

# es.indices.create(index='wikipedia2', body = mapping)
#print(es.indices.exists(index="neo-wikipedia"))
# es.indices.delete('my_index1')
# print(es.get(index="wikipedia",id='-hMkEHABlYU_cnbBMmsk'))
def main():
    def put_data(num,d_id,title,url,doc):
        body = {"doc_ID": d_id, "title": title, "url": url, "text": doc}
        es.index(index="wikipedia",id=num,body=body)

    def Neo_wikipedia():
        with open("/home/amano/tokyo2020+1/data/wikipedia2020.txt",encoding="utf-8") as wiki:
            wikiline = wiki.readline()
            text = []
            count = 0
            wikiid = 0
            while wikiline:
                if wikiline == "\n":
                    wikiline = wiki.readline()
                    continue
                if "</doc>" in wikiline:
                    doc = ''.join(text)
                    # print(title,"\n",doc)
                    res = es.search(index="wikipedia",body={"query": {"term": {"title.keyword":title}}})
                    hits = res['hits']
                    # print("??",hits['total']['value'])
                    if not hits['total']['value'] == 0:
                        print(f"{title}は重複しています．")
                        first_doc = hits['hits'][0]#key
                        print('ヒット数 : %s' % hits['total'])
                        print('タイトル : %s' % first_doc['_source']['title'])
                        if not first_doc['_source']['title'] == title:
                            print("嘘でした，追加します．")
                            put_data(wikiid,title,url,doc)
                    else:
                        print("新しい記事を追加します．")
                        print(f"追加される記事 => {title}")
                        put_data(wikiid,d_id,title,url,doc)
                    # break
                    text = []
                    count = 3
                    wikiid += 1
                if count == 2:
                    text.append(wikiline)
                if count == 1:
                    # print(f"\n {wikiline} \n")
                    title = wikiline.replace("\n","")
                    count = 2
                if "<doc " in wikiline:
                    wikisplit = wikiline.split(" ")
                    d_id = wikisplit[1]
                    d_id = d_id.replace('id=','')
                    d_id = d_id.replace('"','')
                    url = wikisplit[2]
                    url = url.replace('url=','')
                    url = url.replace('"','')
                    # print(url)
                    count = 1
                wikiline = wiki.readline()
    
    def create_jp_index():#日本語用インデックスの登録
        # インデックス作成用JSONの定義
        create_index = {
            "settings": {
                "analysis": {
                    "filter": {
                        "synonyms_filter": { # 同義語フィルターの定義
                            "type": "synonym",
                            "synonyms": [ #同義語リストの定義 (今は空の状態)
                                ]
                        }
                    },
                    "tokenizer": {
                        "kuromoji_w_dic": { # カスタム形態素解析の定義
                        "type": "kuromoji_tokenizer", # kromoji_tokenizerをベースにする
                            # ユーザー辞書としてmy_dic.dicを追加  
                            "user_dictionary": "my_jisho.dic" 
                        }
                    },
                    "analyzer": {
                        "jpn-search": { # 検索用アナライザの定義
                            "type": "custom",
                            "char_filter": [
                                "icu_normalizer", # 文字単位の正規化
                                "kuromoji_iteration_mark" # 繰り返し文字の正規化
                            ],
                            "tokenizer": "kuromoji_w_dic", # 辞書付きkuromoji形態素解析
                            "filter": [
                                "synonyms_filter", # 同義語展開
                                # "kuromoji_baseform", # 活用語の原型化
                                # "kuromoji_part_of_speech", # 不要品詞の除去
                                # "ja_stop", #不要単語の除去
                                "kuromoji_number", # 数字の正規化
                                "kuromoji_stemmer" #長音の正規化
                            ]
                        },
                        "jpn-index": { # インデックス生成用アナライザの定義
                            "type": "custom",
                            "char_filter": [
                                "icu_normalizer", # 文字単位の正規化
                                "kuromoji_iteration_mark" # 繰り返し文字の正規化
                            ],
                            "tokenizer": "kuromoji_w_dic", # 辞書付きkuromoji形態素解析
                            "filter": [
                                # "kuromoji_baseform", # 活用語の原型化
                                # "kuromoji_part_of_speech", # 不要品詞の除去
                                # "ja_stop", #不要単語の除去
                                "kuromoji_number", # 数字の正規化
                                "kuromoji_stemmer" #長音の正規化
                            ]
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "doc_ID": {
                        "type": "integer"
                    },
                    "title": {
                        "type": "keyword"
                    },
                    "url":{
                        "type": "keyword"
                    },
                    "text": {
                        "type": "text",
                        "analyzer": "jpn-index",
                        "search_analyzer": "jpn-search"
                    }
                }
            }
        }

        # 日本語用インデックス名の定義
        jp_index = 'wikipedia'

        # 同じ名前のインデックスがすでにあれば削除する
        if es.indices.exists(index = jp_index):
            print(f"{jp_index}を更新します．")
            es.indices.delete(index = jp_index)
        # インデックス jp_doc の生成
        es.indices.create(index = jp_index, body = create_index)
    create_jp_index() #形態素解析用indexの生成
    Neo_wikipedia()#indexにdocを追加

main()
