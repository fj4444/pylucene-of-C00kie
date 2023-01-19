import lucene
from java.nio.file import Paths
from org.apache.lucene.analysis.cn.smart import SmartChineseAnalyzer
from org.apache.lucene.analysis.core import WhitespaceAnalyzer
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.queryparser.classic import MultiFieldQueryParser
from org.apache.lucene.store import NIOFSDirectory
from org.apache.lucene.search import IndexSearcher,Sort,SortField
from org.apache.lucene.search import BooleanClause

import os
import time
import traceback
import numpy as np

from time import perf_counter


INDEXDIR = os.environ.get('INDEXDIR') if os.environ.get(
    'INDEXDIR') else "./index"
ALARGENUMBER = 100000
FIELD_NUM = 3


def weight_for_time(docs_time: np.ndarray):
    now_ts = int(time.time())
    delta = (now_ts - docs_time) * 1. / (24 * 3600 * 3)          # days from now, dtype=np.float64
    weight = np.exp(-1 * np.sqrt(delta))
    return weight


def time_check(docs_time: np.ndarray, start: str, end: str):
    if start and end:
        print("go to time check.")
        start = int(start)
        end = int(end)
        mask = np.logical_and(docs_time > start, docs_time < end)
        return mask
    else:
        return np.ones_like(docs_time, dtype=bool)
        

class Retriever(object):
    def __init__(self):
        if not lucene.getVMEnv():
            lucene.initVM()
        if not os.path.exists(INDEXDIR):
            os.mkdir(INDEXDIR)
        directory = NIOFSDirectory(Paths.get(INDEXDIR))
        self.searcher = IndexSearcher(DirectoryReader.open(directory))
        # self.analyzer = SmartChineseAnalyzer()
        self.analyzer = WhitespaceAnalyzer()
        # self.cache = Cache(INTERVAL)
        print("retriever created")

    def change_searcher(self,my_directory_reader):
        self.searcher=IndexSearcher(my_directory_reader)
        # self.cache.validate()

    def run(self, keylist, mustlist, mustnotlist, order, start=None, end=None):
        a = self.analyzer
        print("Searching for:" + str(keylist))
        try:
            # tag = 'keys=' + keys + ('&must=' + ','.join(mustlist) if mustlist else '') + \
            #         ('&mustnot=' + ','.join(mustnotlist) if mustnotlist else '') + \
            #         ('&start_time=' + start if start else '') + ('&end_time=' + end if end else '')
            scoreDocs = None
            # if not (scoreDocs := self.cache.get(tag)):
            keys, musts, mustnots = [], [], []
            t_search_start = perf_counter()
            key_num = len(keylist)
            must_num = len(mustlist) if mustlist else 0
            mustnot_num = len(mustnotlist) if mustnotlist else 0
            for key in keylist:
                keys.extend([key] * FIELD_NUM)
            if mustlist:
                for must in mustlist:
                    musts.extend([must] * FIELD_NUM)
            if mustnotlist:
                for mustnot in mustnotlist:
                    mustnots.extend([mustnot] * FIELD_NUM)
            clause = keys + musts + mustnots
            fields = ["text", "name", "keyword"] * (key_num + must_num + mustnot_num)
            flags_should = [BooleanClause.Occur.SHOULD] * FIELD_NUM
            flags_must = [BooleanClause.Occur.MUST, BooleanClause.Occur.SHOULD, BooleanClause.Occur.SHOULD]
            flags_mustnot = [BooleanClause.Occur.MUST_NOT] * FIELD_NUM
            flags = flags_should * key_num + flags_must * must_num + flags_mustnot * mustnot_num
            query = MultiFieldQueryParser.parse(clause, fields, flags, a)
            if(int(order)):#order为一,按照索引时间排序
                sortfield=SortField("time",SortField.Type.INT,True)
                sort=Sort(sortfield)
                topdocs = self.searcher.search(query, ALARGENUMBER,sort)
            else:#否则相关度排序
                topdocs = self.searcher.search(query,ALARGENUMBER,Sort().RELEVANCE)
            scoreDocs = topdocs.scoreDocs
            totolhits = topdocs.totalHits
            print("total matching documents: " + str(totolhits))
            t_search_end = perf_counter()
            print("Search in second: ", t_search_end - t_search_start)
            # self.cache.update(tag, scoreDocs)

            # bottleneck
            t_weighting_start = perf_counter()
            docs = [self.searcher.doc(scoreDoc.doc) for scoreDoc in scoreDocs]
            docs_id = np.array([int(doc.get("id")) for doc in docs], dtype=np.int32)
            docs_time = np.array([int(doc.get("time")) for doc in docs], dtype=np.int32)
            docs_score = np.array([scoreDoc.score for scoreDoc in scoreDocs], dtype=np.float64)

            time_mask = time_check(docs_time, start, end)
            print('original docs id len: ', len(docs_id))
            docs_id = docs_id[time_mask]
            print('docs id len after time check: ', len(docs_id))
            docs_time = docs_time[time_mask]
            docs_score = docs_score[time_mask]
            docs_score = docs_score + weight_for_time(docs_time)

            doc_id_dict = {
                doc_id: doc_score for doc_id, doc_score in zip(docs_id, docs_score)
            }

            doc_id_list = [
                int(sorted_item[0]) for sorted_item in
                (sorted(doc_id_dict.items(), key=lambda kv: (kv[1], kv[0]), reverse=True) 
                    if len(doc_id_dict) else [])
            ]

            t_weighting_end = perf_counter()
            print("Weighing in second: ", t_weighting_end - t_weighting_start)

            return doc_id_list
        except Exception:
            traceback.print_exc()

        