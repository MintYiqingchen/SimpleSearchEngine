import os
from index_constructor import load_doc_records

class DocInfo(object):
    def __init__(self, docid, num_words, rankscore, url):
        self.docid = docid
        self.num_words = num_words
        self.rankscore = rankscore
        self.url = url

class Indexer(object):
    def __init__(self, file_prefix):
       self.file_prefix = file_prefix
       self._load_index()

    def _load_index(self):
        # load dict
        with open(f'{self.file_prefix}.dict.meta') as f:
            self.word2id = {w.strip(): i for i, w in enumerate(f)}
        # load docid2url
        self.docid2url = {}
        self.avg_length = 0
        for docid, num_words, pagerank, url in load_doc_records(f'rank_{self.file_prefix}'):
            self.docid2url[docid] = DocInfo(docid, num_words, pagerank, url)
            self.avg_length += num_words

        self.avg_length /= len(self.docid2url)

    def find_index_item(self, word):
        pass

    def within_window(self, plists):
        pass

    def get_tfidf_scores(self, plists):
        pass

    def get_anchor_scores(self, plists):
        pass

    def and_posting_lists(self, plists):
        pass