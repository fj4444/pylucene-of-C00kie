# coding=utf-8
import os
import MySQLdb
import MySQLdb.cursors
from indexer import Indexer
import traceback
# remote mysql server
HOST = '43.143.194.213'
PORT = 3306
def cut(obj, sec):
    return [obj[i:i+sec] for i in range(0,len(obj),sec)]

class MysqlClient(object):
    def __init__(self):
        try:
            self.conn = MySQLdb.connect(
                host=HOST,
                port=PORT,
                user='root',
                passwd=os.environ.get('MYSQL_PASSWORD'),
                db='backend',
                cursorclass=MySQLdb.cursors.DictCursor
            )
            print("[*] Connection to mysql server established.")
        except Exception as e:
            print("[!] During connection, exception occured: {}".format(e))

    def take_and_index(self):
        idlist=self.check()
        print("check done")
        docdictlist = []
        cursor = self.conn.cursor()
        batchlist=cut(idlist,5000)#每5000个id分成一个batch(一个tuple),这些list组成batchlist
        if batchlist:#有新的新闻
            for batch in batchlist:#每个batch里的id是升序排列的
                maxid=str(batch[-1]["id"])
                minid=str(batch[0]["id"])
                select_sqli="SELECT id, floor(unix_timestamp(TIME)) as time, title, replace(keyword,\"&\",\" \") as keyword, text FROM search_news WHERE id>"+minid+" and id<"+maxid+" and dirty=0"
                change_dirty_sqli = "UPDATE search_news SET dirty = 1 WHERE id>="+minid+" and id<="+maxid+" and dirty=0"
                try:
                    print("start execute")
                    cursor.execute(select_sqli)
                    docdictlist=cursor.fetchall()
                    print(len(docdictlist))
                    indexer=Indexer(docdictlist)
                    print("complete")
                    print(change_dirty_sqli)
                    cursor.execute(change_dirty_sqli)
                    print("changed")
                    del indexer
                except Exception:
                    print(traceback.format_exc())
        cursor.close()
        self.conn.commit()

    def check(self):
        print("start check")
        cur = self.conn.cursor()
        check_sqli = "SELECT id FROM search_news WHERE dirty=0 and id<300"
        # check_sqli = "SELECT id FROM search_news WHERE dirty=0 and id>15000 and id<30000"#小规模测试
        cur.execute(check_sqli)
        idlist = cur.fetchall()
        print(len(idlist))
        cur.close()
        return idlist

    def close(self):
        self.conn.close()
