import json
import sys
import os
import pickle
import struct
from parse_html import Parser

def load_file(filename):
    with open(filename) as f:
        d = json.load(f)

    return d['url'], d['content']

def load_doc_records(file_prefix):
    # docid, num_words, len(url), url
    record_struct = struct.Struct('>III')
    with open(f'{file_prefix}.doc2url.idx', 'rb') as f:
        while True:
            chk = f.read(record_struct.size)
            if chk == b'':
                break
            docid, num_words, urllen = record_struct.unpack(chk)
            chk = f.read(urllen)
            if chk == b'':
                break
            url = struct.unpack(f'>{urllen}s', chk)[0]
            yield docid, num_words, url.decode()

class IndexConstructor(object):
    
    def __init__(self, file_prefix, chunksize = (1 << 31)):
        # invert index
        self.word_index = {}
        self.anchor_index = {}
        # doc index
        self.doc_counter = 0
        self.docid_table = {}
        self.doc_table_file = open(f'{file_prefix}.doc2url.idx', 'wb')
        # constructor status
        self.chunksize = chunksize
        self.tempfile_counter = 0
        self.curr_size = 0
        self.parser = Parser()
        self.file_prefix = file_prefix
        
    
    def __del__(self):
        self.doc_table_file.close()

    def add_file(self, filename):
        url, content = load_file(filename)

        if self.curr_size + len(content) > self.chunksize:
            self._write_temp_index()

        self.curr_size += len(content)
        word_index, anchor_index = self.parser.parse(content)
        # update invert index
        for k, v in word_index.items():
            if k in self.word_index:
                self.word_index[k].extend(v)
            else:
                self.word_index[k] = v
        
        for k, v in anchor_index.items():
            if k in self.anchor_index:
                self.anchor_index[k].extend(v)
            else:
                self.anchor_index[k] = v
        # update doc index
        self._append_doc_record(self.doc_counter, len(word_index), url)
        self.doc_counter += 1

    def _write_temp_index(self):
        pass
    
    def _append_doc_record(self, docid, num_words, url):
        doc_record_template = '>III{}s' # docid, num_words, len(url), ,url
        bin_format = doc_record_template.format(len(url))
        record_struct = struct.Struct(bin_format)
        self.doc_table_file.write(record_struct.pack(docid, num_words, len(url),url.encode()))

    def build_index(self):
        pass


if __name__ == '__main__':
    dir = sys.argv[1]
    constructor = IndexConstructor('test')
    for subdir in os.listdir(dir):
        subdir = os.path.join(dir, subdir)
        for fname in os.listdir(subdir):
            fname = os.path.join(subdir, fname)
            constructor.add_file(fname)
    
    constructor.build_index()

    for a in load_doc_records('test'):
        print(a)
