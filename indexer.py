import json
import sys
import os
import pickle
import struct
import heapq
from parse_html import Parser
from serialize import IndexSerializer, PLNode

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
    
    def __init__(self, file_prefix, chunksize = (1 << 20)):
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
            if len(k) == 0:
                continue
            if k in self.word_index:
                self.word_index[k].append(PLNode(self.doc_counter, v))
            else:
                self.word_index[k] = [PLNode(self.doc_counter, v)]
        
        for k, v in anchor_index.items():
            if len(k) == 0:
                continue
            if k in self.anchor_index:
                self.anchor_index[k].extend(v)
            else:
                self.anchor_index[k] = v
        # update doc index
        self._append_doc_record(self.doc_counter, len(word_index), url)
        self.doc_counter += 1
    
    def _reset_temps(self):
        self.word_index = {}
        self.anchor_index = {}
        self.curr_size = 0
    
    def _write_temp_index(self):
        fname = f'.tmp.{self.tempfile_counter}.{self.file_prefix}.invert.idx'
        with open(fname, 'wb') as f:
            # write word_index
            for k in sorted(self.word_index.keys()):
                self._append_invert_record(f, k, self.word_index[k], False)
        
        fname = f'.tmp.{self.tempfile_counter}.{self.file_prefix}.anchor.idx'
        with open(fname, 'w') as f: # write as text
            for k in sorted(self.anchor_index.keys()):
                f.write(k+ " " + ' '.join(self.anchor_index[k]))
                f.write('\n')

        self.tempfile_counter += 1
        self._reset_temps()
    
    def _append_invert_record(self, f, word, posting_list, with_skip = True, word_id = None):
        bin_posting = IndexSerializer.serialize(posting_list, with_skip)
        
        if word_id: # wordid:len(bin_posting):skiplist:postinglist
            bin_word = struct.pack('>II', word_id, len(bin_posting))
            binarray = bin_word + bin_posting
        else: # len(word):len(bin_posting):word:postinglist
            word_struct = struct.Struct(f'>II{len(word)}s')
            binarray = bytearray(len(bin_posting) + word_struct.size)
            word_struct.pack_into(binarray, 0, len(word), len(bin_posting), word.encode())
            binarray[word_struct.size:] = bin_posting
        
        f.write(binarray)

    
    def _append_doc_record(self, docid, num_words, url):
        doc_record_template = '>III{}s' # docid, num_words, len(url), url
        bin_format = doc_record_template.format(len(url))
        record_struct = struct.Struct(bin_format)
        self.doc_table_file.write(record_struct.pack(docid, num_words, len(url),url.encode()))

    def _merge_anchor_index(self):
        name = '.tmp.{}.'+f'{self.file_prefix}.anchor.idx'
        self.anchor_files = [open(name.format(i), 'r') for i in range(self.tempfile_counter)]


    def merge_index(self):
        self.word2id = {}
        self.urlset = set()
    # 外循环：归并每个word
    # 内循环：归并单个word的多个postinglist
    # 处理完一个word以后，++wordCount产生新的wordId；
    # 处理完一个word以后，压缩它的postinglist写出到全量索引文件；
    # 处理完所有word以后，保存gensim.Dictionary文件；
    # 对于AnchorWordIndex，处理逻辑同上，只不过需要将url替换为docId再写出；

if __name__ == '__main__':
    dir = sys.argv[1]
    constructor = IndexConstructor('test')
    for subdir in os.listdir(dir):
        subdir = os.path.join(dir, subdir)
        for fname in os.listdir(subdir):
            fname = os.path.join(subdir, fname)
            constructor.add_file(fname)
    
    constructor.merge_index()

    for a in load_doc_records('test'):
        print(a)
