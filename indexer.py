import os
import lucene
from org.apache.lucene.store import NIOFSDirectory
from org.apache.lucene.analysis.core import WhitespaceAnalyzer
from org.apache.lucene.analysis.miscellaneous import PerFieldAnalyzerWrapper
from org.apache.lucene.analysis.cn.smart import SmartChineseAnalyzer
# from org.apache.lucene.analysis import TokenStream;
from org.apache.lucene.document import Document, Field, FieldType
from org.apache.lucene.index import IndexWriter, IndexWriterConfig, IndexOptions
from java.nio.file import Paths
from org.apache.lucene.util import Version
from java.util import HashMap
from org.apache.lucene.index import DocValuesType
STOREDIR = os.environ.get('INDEXDIR') if os.environ.get(
    'INDEXDIR') else "./index"


class Indexer(object):
    """
    IndexDocuments to a directory
    """

    def __init__(self, docdict):
        if not os.path.exists(STOREDIR):
            os.mkdir(STOREDIR)

        store = NIOFSDirectory(Paths.get(STOREDIR))
        analyzerPerField=HashMap()
        analyzerPerField.put("keyword",WhitespaceAnalyzer())
        analyzer_wrapper=PerFieldAnalyzerWrapper(SmartChineseAnalyzer(),analyzerPerField)
        config = IndexWriterConfig(analyzer_wrapper)
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND)
        writer = IndexWriter(store, config)
        self.indexDocs(writer, docdict)
        writer.commit()
        writer.close()

    def indexDocs(self, writer, docdict):
        t1 = FieldType()
        t1.setStored(True)
        t1.setTokenized(False)
        t1.setIndexOptions(IndexOptions.DOCS_AND_FREQS)
        t2 = FieldType()
        t2.setStored(False)
        t2.setTokenized(True)
        t2.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)
        t3 = FieldType()
        t3.setStored(True)
        t3.setTokenized(True)
        t3.setIndexOptions(IndexOptions.DOCS_AND_FREQS)
        t4 = FieldType()
        t4.setStored(True)
        t4.setTokenized(False)
        t4.setDocValuesType(DocValuesType.NUMERIC)
        for tfile in docdict:
            try:
                print("Indexing: ", tfile['title'])  # title
                document = Document()
                document.add(Field("id", tfile['id'], t1))
                document.add(Field("time", tfile['time'], t4))
                document.add(Field("name", tfile['title'], t3))
                document.add(Field("keyword", tfile['keyword'], t2))
                document.add(Field("text",tfile['text'],t2))
                writer.addDocument(document)
                print("Done")
            except Exception as e:
                print("failed in indexdocs:", e)
