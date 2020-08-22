from elasticsearch import Elasticsearch
from gensim.models import word2vec
from pyknp import Juman
import os
jumanpp = Juman()
es = Elasticsearch("elasticsearch")

#各ベクトルを抽出して、W2V,BERTのベクトル対応表を作成する。

def main():
    process_org()


def morphological(doc_org):
    result = jumanpp.analysis(doc_org)
    doc_jp = [mrph.midasi for mrph in result.mrph_list()]
    return doc_jp

def transH2Z(text):#半角2全角
        ZEN = "".join(chr(0xff01 + i) for i in range(94))
        HAN = "".join(chr(0x21 + i) for i in range(94))
        # ZEN2HAN = str.maketrans(ZEN, HAN)
        HAN2ZEN = str.maketrans(HAN, ZEN)
        # print(text.translate(HAN2ZEN))
        text = HAN.translate(HAN2ZEN)
        return text

def process_org():
    id_num = 848518
    with open("./wiki_juman.txt","a",encoding="utf-8") as f:
        while True:
            print(id_num)
            doc_org = es.get(index="wikipedia",id=id_num)['_source']['text']
            # print(doc_org)
            if len(doc_org) == 0 or doc_org == "\n":
                id_num = id_num + 1
                continue
            dec_org = transH2Z(doc_org)
            doc_org = doc_org.replace(" ","")#半角スペースはバグの原因
            if len(doc_org.encode('utf-8')) >= 4096:
                print("a")
                doc_sp = doc_org.split("\n")
                for l_snt in doc_sp:
                    if len(l_snt.encode('utf-8')) >= 4096:
                        print("aa")
                        l_snt_sp = l_snt.split("。")
                        for snt in l_snt_sp:
                            print(snt,"\n")
                            if len(snt.encode('utf-8')) >= 4096:
                                print("aaa")
                                snt_kuten = snt.split("、")
                                for snt_k in snt_kuten:
                                    if len(snt_k) == 0 or snt_k == "\n" or len(snt_k.encode('utf-8')) >= 4096:
                                        continue
                                    doc_k = morphological(snt_k)
                                    doc_str_k = ' '.join(doc_k)
                                    print(doc_str_k,end="、 ",file=f)
                            else:
                                if len(snt) == 0 or snt == "\n":
                                    continue
                                doc_jp = morphological(snt)
                                doc_str = ' '.join(doc_jp)
                                print(doc_str,end="。 ",file=f)
                    else:
                        if len(l_snt) == 0 or l_snt == "\n":
                            continue
                        l_doc_jp = morphological(l_snt)
                        l_doc_str = ' '.join(l_doc_jp)
                        print(l_doc_str,end="\n",file=f)
                print("\n",end="",file=f)
                id_num = id_num + 1
            else:
                print("b")
                doc_jp = morphological(doc_org)
                doc_str = ' '.join(doc_jp)
                print(doc_str,end="\n\n",file=f)
                id_num = id_num + 1

                




def multi_process_0():
    doc_lim = es.count(index="wikipedia")['count']
    print("総記事数",doc_lim)
    cpu = os.cpu_count()
    process,process_rem = divmod(doc_lim,cpu)#割り、余り同時出力
    print(process,process_rem)
    with open("./wiki_juman.txt","w",encoding="utf-8") as f:
        for i in range(cpu):
            x = i * process
            y = process * (i + 1)
            print(x,y)
            for j in range(x,y):#ここで重複するyが-1され重複がなくなる。
                doc_org = es.get(index="wikipedia",id=j)['_source']['text']
                # print(doc_org)
                dec_org = transH2Z(doc_org)
                doc_org = doc_org.replace(" ","　")#半角スペースはバグの原因
                doc_jp = morphological(doc_org)
                doc_str = ' '.join(doc_jp)
                print(doc_str,end="\n\n",file=f)

def multi_process_1():
    from concurrent.futures import ThreadPoolExecutor
    def func(keys):
        for i in range(keys[0]):
            x = i * keys[1]
            y = keys[1] * (i + 1)
            print(x,y)
            for j in range(x,y):#ここで重複するyが-1され重複がなくなる。
                doc_org = es.get(index="wikipedia",id=j)['_source']['text']
                # print(doc_org)
                dec_org = transH2Z(doc_org)
                doc_org = doc_org.replace(" ","　")#半角スペースはバグの原因
                doc_jp = morphological(doc_org)
                doc_str = ' '.join(doc_jp)
                print(doc_str,end="\n\n",file=f)
    doc_lim = es.count(index="wikipedia")['count']
    print("総記事数",doc_lim)
    cpu = os.cpu_count()
    process,process_rem = divmod(doc_lim,cpu)#割り、余り同時出力
    print(process,process_rem)
    keys = [cpu,process]
    with open("./wiki_juman.txt","w",encoding="utf-8") as f:
        pool = ThreadPoolExecutor(cpu)
        pool.submit(func,keys)
            

main()
