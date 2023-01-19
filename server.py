from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs, unquote
from retriever import Retriever
import traceback
import json
import re

hostName = "0.0.0.0"
serverPort = 8080


def getlist(dict, label):
    print(dict.__contains__(label))
    return dict[label][0] if dict.__contains__(label) else None


def check(querys):
    '''若query中不含汉字/字母或数字则不合法'''
    exgR = "[\u4e00-\u9fffa-zA-Z0-9]"
    pattern = re.compile(exgR)
    # print(querys)
    if querys == "":
        return False
    querykeylist = querys.split(",")
    for query in querykeylist:
        if not re.search(pattern, query):  # 不含合法字符
            if not query==" ":
                return False
    return True


def my_update(dict1, dict2):
    try:
        samekeyset = dict1.keys() & dict2.keys()
        if len(samekeyset):
            for item in samekeyset:
                dict2[item] += dict1[item]
        dict1.update(dict2)
        return dict1
    except Exception:
        traceback.print_exc()
    

class MyWebServer(BaseHTTPRequestHandler):

    stc_retriever = Retriever()

    @classmethod
    def change_retriever(cls, my_directory_reader):
        cls.stc_retriever.change_searcher(my_directory_reader)

    def do_GET(self):
        try:
            # params = urlparse(self.path)
            querys = parse_qs(urlparse(self.path).query)
            print(querys)
            keys = getlist(querys, "key")
            must = getlist(querys, "must")
            mustnot = getlist(querys, "mustnot")
            start_time = getlist(querys, "start_time")
            end_time = getlist(querys, "end_time")
            order = getlist(querys,"order")
            if not check(keys):
                self.send_response(400)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps("Query illegal").encode())
            else:
                keylist = keys.split(',')
                mustlist = must.split(',') if must else None
                mustnotlist = mustnot.split(',') if mustnot else None
                retriever = self.stc_retriever
                doc_id_list = retriever.run(keylist, mustlist, mustnotlist, order, start_time, end_time)
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(doc_id_list).encode())
        except Exception:
            traceback.print_exc()


