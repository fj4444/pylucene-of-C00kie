from db import MysqlClient
from indexer import Indexer
from server import MyWebServer
import lucene
from org.apache.lucene.store import NIOFSDirectory
from org.apache.lucene.index import DirectoryReader
from http.server import HTTPServer
import time, os, _thread
from java.nio.file import Paths
from utils import INTERVAL

hostName = "0.0.0.0"
serverPort = 8080
INDEXDIR = os.environ.get('INDEXDIR') if os.environ.get(
    'INDEXDIR') else "./index"
SHOULD_SERVE = True

def indexing_and_update():
    vm_env = lucene.getVMEnv()
    vm_env.attachCurrentThread()
    directory = NIOFSDirectory(Paths.get(INDEXDIR))
    my_directory_reader = DirectoryReader.open(directory)
    while True:
        db = MysqlClient()
        db.take_and_index()
        db.close()
        del db
        my_new_directory_reader = DirectoryReader.openIfChanged(my_directory_reader)
        if my_new_directory_reader: # 有更新
            my_directory_reader = my_new_directory_reader
            MyWebServer.change_retriever(my_directory_reader)
        time.sleep(INTERVAL)


def start_serve(webServer):
    try:
        webServer.serve_forever()
    except KeyboardInterrupt:
        pass
    webServer.server_close()
    print("Server stopped.")

if __name__ == "__main__":
    # start JVM
    if not lucene.getVMEnv():
        lucene.initVM()
        print("started VM")

    # indexing & initialize server
    _thread.start_new_thread(indexing_and_update,())
    webServer = HTTPServer((hostName, serverPort), MyWebServer)
    print("Server created http://%s:%s" % (hostName, serverPort))
    
    start_serve(webServer)
