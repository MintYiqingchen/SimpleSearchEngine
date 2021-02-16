import os
import json

SKIPSIZE = 3


class PLNode:

    def __init__(self, docId, occurrences):
        self.docId = docId
        self.occurrences = occurrences


class indexConstructor:

    def __init__(self, wordIndex, anchorIndex):
        self.wordIndex = wordIndex
        self.anchorIndex = anchorIndex

    def serialize(PostingList):
        # byte = bytes()
        skipDictionary = {}
        idx = 0
        length = 0
        listByte = bytes()
        for plnode in PostingList:
            listByte += plnode.docId.to_bytes(4, "big")
            listByte += len(plnode.occurrences).to_bytes(4, "big")
            for i in plnode.occurrences:
                for j in i:
                    listByte += j.to_bytes(4, "big")

            if idx == SKIPSIZE:
                skipDictionary[plnode.docId] = length
                idx = 0
                # length = 0
            else:
                idx += 1
                length += (4+len(plnode.occurrences)*8)

        # print(skipDictionary)
        dictByte = bytes()
        dictByte += len(skipDictionary).to_bytes(4, "big")
        for key in skipDictionary:
            dictByte += key.to_bytes(4, "big")
            dictByte += skipDictionary[key].to_bytes(4, "big")

        return dictByte + listByte

    def deserialize(byte):
        dictLen = int.from_bytes(byte[:4], "big")
        skipDictionary = {}
        idx = 4
        for i in range(dictLen):
            skipDictionary[int.from_bytes(
                byte[idx:idx+4], "big")] = int.from_bytes(byte[idx+4:idx+8], "big")
            idx += 8
        print(dictLen)
        print(skipDictionary)
        postingList = []
        while(idx < len(byte)):
            docId = int.from_bytes(byte[idx:idx+4], "big")
            idx += 4
            occLen = int.from_bytes(byte[idx:idx+4], "big")
            idx += 4
            print("{0} {1}".format(docId, occLen))
            occurrencelist = []
            for i in range(occLen):
                occurrencelist.append([int.from_bytes(
                    byte[idx:idx+4], "big"), int.from_bytes(byte[idx+4:idx+8], "big")])
                idx += 8
            postingList.append(PLNode(docId, occurrencelist))
            print(docId)
            print(occurrencelist)

        return skipDictionary, postingList


if __name__ == "__main__":
    pln1 = PLNode(1, [[0, 1], [9, 4], [30, 0], [35, 0]])
    pln2 = PLNode(2, [[5, 1], [13, 2], [38, 3],
                      [49, 0], [67, 0], [90, 2], [107, 2]])
    pln3 = PLNode(3, [[7, 1], [29, 2], [38, 0],
                      [57, 0], [60, 1], [100, 2], [1002, 2], [1013, 2], [1022, 2], [1122, 2], [1222, 2]])
    pln4 = PLNode(4, [[0, 1], [9, 4], [30, 0], [35, 0]])
    pln5 = PLNode(5, [[5, 1], [13, 2], [38, 3],
                      [49, 0], [67, 0], [90, 2], [107, 2]])
    pln6 = PLNode(6, [[7, 1], [29, 2], [38, 0],
                      [57, 0], [60, 1], [100, 2], [1002, 2], [1013, 2], [1022, 2], [1122, 2], [1222, 2]])
    pln7 = PLNode(7, [[0, 1], [9, 4], [30, 0], [35, 0]])
    pln8 = PLNode(8, [[5, 1], [13, 2], [38, 3],
                      [49, 0], [67, 0], [90, 2], [107, 2]])
    pln9 = PLNode(9, [[7, 1], [29, 2], [38, 0],
                      [57, 0], [60, 1], [100, 2], [1002, 2], [1013, 2], [1022, 2], [1122, 2], [1222, 2]])
    pl = [pln1, pln2, pln3, pln4, pln5, pln6, pln7, pln8, pln9]

    # print(len(pln1.occurrences))
    # print(len(pln2.occurrences))
    byte = indexConstructor.serialize(pl)
    print(byte)
    indexConstructor.deserialize(byte)

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
