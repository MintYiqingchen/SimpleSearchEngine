import json
import sys
import os
import pickle
import struct
import heapq
import glob
from collections import Counter
from parse_html import Parser, is_valid
from serialize import IndexSerializer, PLNode
from utils import get_logger

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
    
    def __init__(self, file_prefix, chunksize = (1 << 26)):
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

        self.logger = get_logger('INDEX_CONSTRUCTOR')

    def add_file(self, filename):
        url, content = load_file(filename)
        if not is_valid(url):
            self.logger.info(f'{url}, {filename}')
            return
        if len(content) >= (1<<25):
            self.logger.info('too large ', len(content), f' {url}, {fname}')
            return

        if self.curr_size + len(content) > self.chunksize:
            self._write_temp_index()
        
        print(filename)
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
        if self.curr_size == 0:
            return

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
            print(word)
        
        f.write(binarray)

    def _append_anchor_record(self, f, wid, docIds):
        f.write(IndexSerializer.simple_serialize(wid))
        f.write(IndexSerializer.simple_serialize(docIds))

    def _append_doc_record(self, docid, num_words, url):
        doc_record_template = '>III{}s' # docid, num_words, len(url), url
        bin_format = doc_record_template.format(len(url))
        record_struct = struct.Struct(bin_format)
        self.doc_table_file.write(record_struct.pack(docid, num_words, len(url),url.encode()))

    def _append_dict_record(self, df, word):
        df.write(word+'\n')

    def _append_offset_record(self, of, invert_offset, anchor_offset = -1):
        of.write(struct.pack('>qq', invert_offset, anchor_offset))

    def _merge_anchor_index(self):
        name = '.tmp.{}.'+f'{self.file_prefix}.anchor.idx'
        anchor_files = [open(name.format(i), 'r') for i in range(self.tempfile_counter)]
        
        def read_next_heapitem(files, i, word2id):
            while True:
                line = files[i].readline().strip()
                if len(line) == 0:
                    # print('finish anchor file ', i)
                    return None
                a = line.split(' ', 1)
                if a[0] not in word2id:
                    continue
                return word2id[a[0]], i, a[1]

        # initialize heap
        heap = []
        for i in range(len(anchor_files)):
            a = read_next_heapitem(anchor_files, i, self.word2id)
            if a:
                heapq.heappush(heap, a)
        
        
        with open(f'{self.file_prefix}.anchor.idx', 'wb') as anchorFile:
            while heap:
                currid = heap[0][0]
                counter = Counter()
                while heap and heap[0][0] == currid: # merge same word
                    filei = heap[0][1]
                    urls = map(lambda u: self.url2docid[u], filter(lambda u: u in self.url2docid, heap[0][2].split()))
                    counter.update(urls)

                    a = read_next_heapitem(anchor_files, filei, self.word2id)
                    if a:
                        heapq.heappush(heap, a)
                    else:
                        heapq.heappop(heap)
                # write record
                fileOffset = anchorFile.tell()
                self._append_anchor_record(anchorFile, currid, counter)
                self.offsets[currid][1] = fileOffset

        for f in anchor_files:
            f.close()

    def _merge_invert_index(self):
        name = '.tmp.{}.'+f'{self.file_prefix}.invert.idx'
        index_files = [open(name.format(i), 'rb') for i in range(self.tempfile_counter)]

        def read_next_heapitem(files, i):
            if files[i] is None:
                return None
            rsize = struct.calcsize('>II')
            buffer = files[i].read(rsize)
            if len(buffer) < rsize:
                return None
            wsize, psize = struct.unpack('>II', buffer)
            buffer = files[i].read(wsize + psize)
            if len(buffer) < wsize + psize:
                return None
            word = buffer[:wsize].decode()
            plist = IndexSerializer.deserialize(buffer, False, wsize)
            return word, i, plist

        # initialize heap
        heap = []
        for i in range(len(index_files)):
            a = read_next_heapitem(index_files, i)
            if a:
                heapq.heappush(heap, a)
        
        word_count = 0
        with open(f'{self.file_prefix}.invert.idx', 'wb') as invertFile, \
            open(f'{self.file_prefix}.dict.meta', 'w') as dictFile:

            while heap:
                currword = heap[0][0]
                plists = []
                while heap and heap[0][0] == currword: # merge same word
                    filei = heap[0][1]
                    plists.append(heap[0][2])
                    a = read_next_heapitem(index_files, filei)
                    if a:
                        heapq.heappush(heap, a)
                    else:
                        heapq.heappop(heap)

                plists = self._merge_posting_list(plists)
                # write record
                indexOffset = invertFile.tell()
                self._append_invert_record(invertFile, currword, plists, True, word_count)
                self._append_dict_record(dictFile, currword)
                self.word2id[currword] = word_count
                self.offsets.append([indexOffset, -1])
                word_count += 1

        for f in index_files:
            f.close()

    def _merge_posting_list(self, plists):
        def read_next_heapitem(plists, i):
            try:
                return next(plists[i]), i
            except StopIteration:
                return None

        plists = [iter(p) for p in plists]
        # initialize heap
        heap = []
        for i in range(len(plists)):
            a = read_next_heapitem(plists, i)
            if a:
                heapq.heappush(heap, a)

        res = []
        while heap:
            res.append(heap[0][0])
            a = read_next_heapitem(plists, heap[0][1])
            if a:
                heapq.heappush(heap, a)
            else:
                heapq.heappop(heap)

        return res

    def merge_index(self):
        self._write_temp_index()
        self.word2id = {}
        self.offsets = []
        self._merge_invert_index()

        self.url2docid = {a[2]: a[0] for a in load_doc_records(self.file_prefix)}
        self._merge_anchor_index()

        with open(f'{self.file_prefix}.offset.meta', 'wb') as offsetFile:
            for a, b in self.offsets:
                self._append_offset_record(offsetFile, a, b)

        # for fname in glob.iglob('.tmp*.idx'):
        #     os.remove(fname)
    # outer loop：merge word
    # inner loop：merge postinglists of each word
    # after handling each word，++wordCount in order to generate new wordId；
    #   compress postinglist, write to full index file
    # after handling all words, write out offset file and dictionary

# Submit a report (pdf) with the following content: a table with the following numbers pertaining to your index. It should have, at least the number of documents, the number of [unique] words, and the total size (in KB) of your index on disk.
if __name__ == '__main__':
    dir = sys.argv[1]
    constructor = IndexConstructor('test')
    count = 0
    for subdir in os.listdir(dir):
        subdir = os.path.join(dir, subdir)
        for fname in os.listdir(subdir):
            count += 1
            fname = os.path.join(subdir, fname)
            constructor.add_file(fname)
    print("finish parse stage: ", count)
    
    constructor.merge_index()
    print('document number: ', len(constructor.url2docid))
    # for a in load_doc_records('test'):
    #     print(a)
