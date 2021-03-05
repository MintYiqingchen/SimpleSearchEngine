import os
from index_constructor import load_doc_records
from utils import get_logger
import struct
from serialize import IndexSerializer
from collections import Counter, defaultdict
import heapq
import math

class DocInfo(object):
    def __init__(self, docid, num_words, rankscore, url):
        self.docid = docid
        self.num_words = num_words
        self.rankscore = rankscore
        self.url = url
    
class Indexer(object):
    def __init__(self, file_prefix):
        self.logger = get_logger('INDEXER')
        self.anchor_weight = 9
        self.window_weight = 5
        self.pagerank_weight = 180
        self.topk = 200
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
        max_rank = max(self.docid2url.values(), key=lambda x: x.rankscore)
        min_rank = min(self.docid2url.values(), key=lambda x: x.rankscore)
        for v in self.docid2url.values():
            v.rankscore = (v.rankscore - min_rank.rankscore) / (max_rank.rankscore - min_rank.rankscore + 1)

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

    '''
    calculate tf-idf given list of posting lists for each term

    @:param plists: [pl1, pl2, ...]
    @:return sum_list: [(docid, tfidf_sum)]
    '''
    def get_tfidf_scores(self, plists):
        tfidf_dict = {}
        for i in plists:
            tfidf_dict = self._term_tfidf(i, tfidf_dict)
        return tfidf_dict
        # return self._transform_score_format(tfidf_dict)

    '''
    calculate tf-idf term-at-a-time

    @:param pl: posting list of one term
    @:param dict: tfidf_dict(k: docid, v: tfidf_sum)
    @:return: tfidf_dict
    '''

    def _term_tfidf(self, pl, dict):
        n = len(self.docid2url)
        for i in pl:
            docid = i.docId
            tf = i.tf
            doc_num = len(pl)
            tfidf = tf * math.log10(n / doc_num)
            if docid not in dict:
                dict[docid] = tfidf
            else:
                dict[docid] += tfidf
        return dict

    '''
    calculate the sum of tf-idf for each doc

    @:param dict: summed tf-idf dict(k: docid, v: tfidf sum)
    @:return sum_list: [(docid, tfidf_sum)]
    '''

    def _transform_score_format(self, dict):
        score_list = []
        for docid in dict:
            tfidf_sum = dict[docid]
            score_list.append((docid, tfidf_sum))
        return score_list

    '''
    calculate the anchor score

    @:param plits: list of dict: docIds[docid] = count of word in this docid
    @:return: list[(int,double)] docid and score

    '''

    def get_anchor_scores(self, plists):
        total = 0
        docid_count = {}  # k: doc id, v: count
        print(plists)
        for dict in plists:
            for docid in dict:  # each doc id
                count = dict[docid]  # count for each doc id of each word
                total += count
                if docid not in docid_count:
                    docid_count[docid] = count
                else:
                    docid_count[docid] += count

        for docid in docid_count:
            docid_count[docid] /= total
            # note: if plist contain lots of word, the total is large

        return docid_count

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

    def scale_score(self, score_dict):
        max_score = max(score_dict.values())
        min_score = min(score_dict.values())

        for k in score_dict:
            score_dict[k] = (score_dict[k] - min_score)/(max_score - min_score + 1)
        return score_dict

    def get_result(self, words): # called by app
        plists = [self.find_index_item(w)[1] for w in words] # [(skipdict, plist)]
        score_dict = self.get_tfidf_scores(plists) # dict(docid->tfidf)

        common_map = self.and_posting_lists(plists) # dict(docid->list[idx])
        if common_map and len(words) > 1:
            window_sizes = self.within_window(plists, common_map) # dict(docid->list[window_size])
            # print(window_sizes)
            for docid, wsize in window_sizes.items():
                score_dict[docid] += self.window_weight / max(1, wsize - len(words))
        
        # anchor score
        plists = list(filter(lambda x: x is not None, (self.find_anchor_item(w) for w in words)))
        anchor_score = self.get_anchor_scores(plists)
        for docid, score in anchor_score.items():
            if docid in score_dict:
                score_dict[docid] += score * self.anchor_weight
            else:
                score_dict[docid] = score * self.anchor_weight
        
        # page rank score
        for docid in score_dict:
            score_dict[docid] += self.docid2url[docid].rankscore * self.pagerank_weight

        # top-k
        hp = []
        for docid, score in score_dict.items():
            heapq.heappush(hp, (score, docid))
            if len(hp) > self.topk:
                heapq.heappop(hp)
        res = [None] * len(hp)
        while hp:
            score, docid = heapq.heappop(hp)
            res[len(hp)] = {"docid": docid, "score": score, "url": self.docid2url[docid].url}
        return res


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

    # # test get_tfidf_scores
    from serialize import PLNode
    pln1 = PLNode(1, 0.5, [0, 9, 30, 35])
    pln2 = PLNode(2, 0.88, [5, 13, 38, 49, 67, 90, 107, 2])
    pln3 = PLNode(3, 0.23, [7, 29, 38, 57, 60])
    pl1 = [pln1, pln2]
    pl2 = [pln2, pln3]
    indexer.get_tfidf_scores([pl1, pl2])
    
    # get_anchor_scores
    dict1={1:2, 2:1}
    dict2 = {2: 2, 3: 4}
    anchor_ls = [dict1, dict2]
    indexer.get_anchor_scores(anchor_ls)