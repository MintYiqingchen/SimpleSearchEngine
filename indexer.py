import os
from index_constructor import load_doc_records
from utils import get_logger
import struct
from serialize import IndexSerializer
from collections import Counter, defaultdict
import heapq

class DocInfo(object):
    def __init__(self, docid, num_words, rankscore, url):
        self.docid = docid
        self.num_words = num_words
        self.rankscore = rankscore
        self.url = url

class Indexer(object):
    def __init__(self, file_prefix):
        self.logger = get_logger('INDEXER')

        self.file_prefix = file_prefix
        self._load_index()

    def _load_index(self):
        # load dict
        with open(f'{self.file_prefix}.dict.meta') as f:
            self.word2id = {w.strip(): i for i, w in enumerate(f)}
        # load offsets: memory unfriendly
        self.wid2offsets = {}
        with open(f'{self.file_prefix}.offset.meta', 'rb') as f:
            count = 0
            while True:
                b = f.read(16)
                if len(b) < 16:
                    break
                self.wid2offsets[count] = struct.unpack('>qq', b)
                count += 1

        # load docid2url
        self.docid2url = {}
        self.avg_length = 0
        for docid, num_words, pagerank, url in load_doc_records(f'rank_{self.file_prefix}'):
            self.docid2url[docid] = DocInfo(docid, num_words, pagerank, url)
            self.avg_length += num_words

        self.avg_length /= len(self.docid2url)
        self.logger.info(f'num doc: {len(self.docid2url)}, doc avg len: {self.avg_length}, num words: {len(self.word2id)}, {len(self.wid2offsets)}')

    def find_index_item(self, word):
        wordid = self.word2id[word]
        offset = self.wid2offsets[wordid][0]
        with open(f'{self.file_prefix}.invert.idx', 'rb') as f:
            f.seek(offset)
            buffer = f.read(8)
            wordid, postlen = struct.unpack('>II', buffer)
            postlistBuffer = f.read(postlen)
            return IndexSerializer.deserialize(postlistBuffer)

    def find_anchor_item(self, word):
        wordid = self.word2id[word]
        offset = self.wid2offsets[wordid][1]
        if (offset == -1):
            return None
        with open(f'{self.file_prefix}.anchor.idx', 'rb') as f:
            f.seek(offset)
            buffer = f.read(8)
            wordid, dictlen = struct.unpack('>II', buffer)
            docIds = {}
            for i in range(dictlen):
                buffer = f.read(4)
                k = struct.unpack('>I', buffer)[0]
                buffer = f.read(4)
                v = struct.unpack('>I', buffer)[0]
                docIds[k] = v
            return docIds
    
    def within_window(self, plists, common_map):
        def read_next_heapitem(iters, i):
            try:
                return next(iters[i]), i
            except StopIteration:
                return None
        wsize_map = {}
        for docid, idxs in common_map.items():
            left = -1
            right = -1
            # initialize window
            iters = [iter(plists[pi][idx].occurrences) for pi, idx in enumerate(idxs)]
            hp = []
            for pi in range(len(iters)):
                a = read_next_heapitem(iters, pi)
                hp.append(a)
                if a[0] < left or left < 0:
                    left = a[0]
                if a[0] > right or right < 0:
                    right = a[0]
            heapq.heapify(hp)
            window_size = right - left + 1
            # sliding window
            while len(hp) == len(idxs):
                position, pi = heapq.heappop(hp)
                # if position < 0:
                #     self.logger.error(f'{position} {pi}')
                a = read_next_heapitem(iters, pi)
                if a:
                    heapq.heappush(hp, a)
                    left = hp[0][0]
                    right = max(right, a[0])
                    window_size = min(window_size, right - left + 1)

            if right >= 0 and left >= 0:
                wsize_map[docid] = window_size
        return wsize_map

    def get_tfidf_scores(self, plists):
        pass

    def get_anchor_scores(self, plists):
        pass

    def and_posting_lists(self, plists): # return [plist]
        if len(plists) <= 1:
            return plists
        counter = Counter()
        indexdict = defaultdict(list)
        for i, d in enumerate(plists[0]):
            counter[d.docId] = 1
            indexdict[d.docId].append(i)

        for pi in range(1, len(plists)):
            # print(skipdict)
            for i, d in enumerate(plists[pi]):
                if d.docId not in counter:
                    continue
                elif counter[d.docId] != pi: # exists in previous words' posting list
                    del counter[d.docId]
                    del indexdict[d.docId]
                else:
                    counter[d.docId] += 1
                    indexdict[d.docId].append(i)

        common_id = []
        while len(counter):
            docId, count = counter.most_common(1)[0]
            # print(docId)
            # print(count)
            if count < len(plists):
                break
            common_id.append(docId)
            del counter[docId]
        
        if not common_id: # and result is empty
            return None

        return {k: indexdict[k] for k in common_id}

    def get_result(self, words): # called by app
        plists = [self.find_index_item(w)[1] for w in words] # [(skipdict, plist)]
        
        common_map = self.and_posting_lists(plists) # dict(docid->list[idx])
        if common_map and len(words) > 1:
            window_sizes = self.within_window(plists, common_map) # dict(docid->list[window_size])
            # print(window_sizes)
        return []

if __name__ == '__main__':
    indexer = Indexer('test')
    print(indexer.wid2offsets[130758])
    _, pl = indexer.find_index_item('dork');
    print(pl)
    # for i in pl:
    #     print(i.docId)

    apl = indexer.find_anchor_item('visit');
    # indexer.find_anchor_item('visit');
    print(apl)
    for i in apl:
        print(i)