import os
import json
import struct
# from indexer import Indexer

SKIPSIZE = 512
class PLNode:
    def __init__(self, docId, tf, occurrences):
        self.docId = docId
        self.tf = tf
        self.occurrences = occurrences
    def __eq__(self, value):
        if self.docId != value.docId:
            return False
        if self.tf != value.tf:
            return False
        if self.occurrences != value.occurrences:
            return False
        return True

    def __lt__(self, value):
        if isinstance(value, int):
            return self.docId < value
        return self.docId < value.docId
    
    def __gt__(self, value):
        if isinstance(value, int):
            return self.docId > value
        return self.docId > value.docId

class IndexSerializer(object):
    @staticmethod
    def simple_serialize(obj):
        if isinstance(obj, str):
            return struct.pack('>I', len(obj)) + obj.encode()
        if isinstance(obj, int):
            return struct.pack('>I', obj)
        if isinstance(obj, list): # only list[int]
            return IndexSerializer.simple_serialize(len(obj)) + b''.join(IndexSerializer.simple_serialize(item) for item in obj)
        if isinstance(obj, dict): # only dick[int]->int
            return IndexSerializer.simple_serialize(len(obj)) + b''.join(IndexSerializer.simple_serialize(k) + IndexSerializer.simple_serialize(v) for k, v in obj.items())
        raise NotImplementedError
    
    @staticmethod
    def simple_deserialize(bt, tp, offset = 0):
        if tp == int:
            return struct.unpack_from('>I', bt, offset)[0]
        raise NotImplementedError
    
    @staticmethod
    def serialize(PostingList, with_skip = True):
        skipDictionary = {}
        length = 0
        listByte = bytes()
        for idx, plnode in enumerate(PostingList):
            if with_skip:
                if (idx+1) % SKIPSIZE == 0:
                    skipDictionary[plnode.docId] = len(listByte)

            listByte += plnode.docId.to_bytes(4, "big")
            listByte += struct.pack('>d', plnode.tf)
            listByte += len(plnode.occurrences).to_bytes(4, "big")
            for i in plnode.occurrences:
                listByte += i.to_bytes(4, "big")

        if with_skip:
            # print(skipDictionary)
            dictByte = bytes()
            dictByte += len(skipDictionary).to_bytes(4, "big")
            for key in skipDictionary:
                dictByte += key.to_bytes(4, "big")
                dictByte += skipDictionary[key].to_bytes(4, "big")

            return dictByte + listByte

        return listByte

    @staticmethod
    def deserialize(byte, with_skip = True, offset = 0):
        if with_skip:
            dictLen = struct.unpack_from(">I", byte, offset)[0]
            # dictLen = int.from_bytes(byte[offset: offset + 4], "big")
            skipDictionary = {}
            idx = offset + 4
            for i in range(dictLen):
                skipDictionary[struct.unpack_from(">I", byte, idx)[0]] = struct.unpack_from(">I", byte, idx+4)[0]
                # skipDictionary[int.from_bytes(
                #     byte[idx:idx+4], "big")] = int.from_bytes(byte[idx+4:idx+8], "big")
                idx += 8
            # print(dictLen)
            # print(skipDictionary)
        else:
            idx = offset

        postingList = []
        while(idx < len(byte)):
            docId = struct.unpack_from(">I", byte, idx)[0]
            # docId = int.from_bytes(byte[idx:idx+4], "big")
            idx += 4
            tf = struct.unpack_from('>d', byte, idx)[0]
            idx += 8
            occLen = struct.unpack_from(">I", byte, idx)[0] # int.from_bytes(byte[idx:idx+4], "big")
            idx += 4
            # print("{0} {1}".format(docId, occLen))
            occurrencelist = []
            for i in range(occLen):
                occurrencelist.append(struct.unpack_from(">I", byte, idx)[0])
                # occurrencelist.append(int.from_bytes(
                #     byte[idx:idx+4], "big"))
                idx += 4
            postingList.append(PLNode(docId, tf, occurrencelist))
            # print(docId)
            # print(occurrencelist)
        
        if with_skip:
            return skipDictionary, postingList
        
        return postingList  


if __name__ == "__main__":
    pln1 = PLNode(1, 0.5, [0, 9, 30, 35])
    pln2 = PLNode(2, 0.88, [5, 13, 38, 49, 67, 90, 107, 2])
    pln3 = PLNode(3, 0.23, [7, 29, 38, 57, 60])
    # pln4 = PLNode(4, [[0, 1], [9, 4], [30, 0], [35, 0]])
    # pln5 = PLNode(5, [[5, 1], [13, 2], [38, 3],
    #                   [49, 0], [67, 0], [90, 2], [107, 2]])
    # pln6 = PLNode(6, [[7, 1], [29, 2], [38, 0],
    #                   [57, 0], [60, 1], [100, 2], [1002, 2], [1013, 2], [1022, 2], [1122, 2], [1222, 2]])
    # pln7 = PLNode(7, [[0, 1], [9, 4], [30, 0], [35, 0]])
    # pln8 = PLNode(8, [[5, 1], [13, 2], [38, 3],
    #                   [49, 0], [67, 0], [90, 2], [107, 2]])
    # pln9 = PLNode(9, [[7, 1], [29, 2], [38, 0],
    #                   [57, 0], [60, 1], [100, 2], [1002, 2], [1013, 2], [1022, 2], [1122, 2], [1222, 2]])
    pl = [pln1, pln2, pln3] # , pln4, pln5, pln6, pln7, pln8, pln9]

    # print(len(pln1.occurrences))
    # print(len(pln2.occurrences))
    byte = IndexSerializer.serialize(pl, True)
    # print(byte)
    _, a = IndexSerializer.deserialize(byte, True)
    print(a == pl)

    import bisect
    a = bisect.bisect(pl, 0)
    print(a)
# b = pln1.docId.to_bytes(4, "big")
# # b += bytes(pln1.occurrences)
# b2 = bytes()
# for i in pln1.occurrences:
#     for j in i:
#         b2 += j.to_bytes(4, "big")

# for i in range(0, len(b2), 4):
#     # print(b2[i:i+4])
#     print(int.from_bytes(b2[i:i+4], "big"))
# print(b)
# print(b2)
# print(int.from_bytes(b, "big"))
# i = os.listdir("/Users/zhiyutao/Downloads/DEV")
# print(os.listdir("/Users/zhiyutao/Downloads/DEV"+"/" + i[0]))
