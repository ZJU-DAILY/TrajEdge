# https://github.com/rottenivy
# This Repository is under Apache License. See LICENSE file for full license text.
import math
import os

from src.main.java.org.example.bolt.compression.utils import network_data, numDirection


def toBit(num: int):
    return "{0:0>{1}}".format(int(bin(num)[2:]), Q)


def toBitWithWidth(num: int, width):
    # assert num < 2 ^ width
    return "{0:0>{1}}".format(int(bin(num)[2:]), width)


def NaiveClockWise(node_path):
    oriMethod = ''
    excludeEdge = [node_path[0], node_path[1]]
    for i in range(2, len(node_path)):
        d = numDirection(roadnetwork, vertice_dict, nodes[i - 2], nodes[i - 1], nodes[i - 1], nodes[i])
        oriMethod += toBit(d)
    return excludeEdge, oriMethod


def NaiveClockWiseDecompress(excludeEdge, bitArray):
    decompressT = [excludeEdge[0], excludeEdge[1]]
    baseEdge = excludeEdge
    for i in range(0, len(bitArray), 3):
        all_nodes = roadnetwork.neighbors(baseEdge[1])
        direction = int(bitArray[i:i + 3], 2)
        for node in all_nodes:
            if node != baseEdge[0]:
                d = numDirection(roadnetwork, vertice_dict, baseEdge[0], baseEdge[1], baseEdge[1], node)
                if d == direction:
                    baseEdge = [baseEdge[1], node]
                    decompressT.append(node)
                    break
    return decompressT


def DORClockWise(node_path):
    D, F, To = '', '', ''
    gLength = 0
    excludeEdges = [[node_path[0], node_path[1]], [node_path[1], node_path[2]]]
    maxLen = 0
    for i in range(3, len(node_path)):
        d0 = numDirection(roadnetwork, vertice_dict, nodes[i - 2], nodes[i - 1], nodes[i - 1], nodes[i])
        d1 = numDirection(roadnetwork, vertice_dict, nodes[i - 3], nodes[i - 2], nodes[i - 1], nodes[i])
        F += toBit(d0)

        if d1 == 0 or d0 == 0:
            delta = '0' if d0 == 0 else '1'
            D += delta
            gLength += 1
        else:
            if gLength >= 1 and gLength + Q + 1 + 4 < gLength * Q:
                tmp = Sdm + toBitWithWidth(gLength, 4) + D
                print(tmp)
                To += tmp
                maxLen = max(maxLen, gLength)
            else:
                To += F
            D, F = '', ''
            gLength = 0
    print("Max length: " + str(maxLen))
    return excludeEdges, To


def DORClockWiseDecompress(excludeEdges, bitArray):
    decompressT = [excludeEdges[0][0], excludeEdges[0][1], excludeEdges[1][1]]
    baseEdge1 = excludeEdges[0]
    baseEdge2 = excludeEdges[1]
    i = 0
    while i < len(bitArray):
        if i + 4 < len(bitArray) and bitArray[i:i + 4] == Sdm:
            length = int(bitArray[i + 4:i + 4 + 4], 2)
            ds = bitArray[i + 4 + 4:i + 4 + 4 + length]
            i += 4 + 4 + length
            for c in ds:
                if c == '0':
                    baseEdge = baseEdge2
                else:
                    baseEdge = baseEdge1
                all_nodes = roadnetwork.neighbors(baseEdge[1])
                direction = 0
                for node in all_nodes:
                    if node != baseEdge[0]:
                        d = numDirection(roadnetwork, vertice_dict, baseEdge[0], baseEdge[1], baseEdge[1], node)
                        if d == direction:
                            baseEdge = [baseEdge[1], node]
                            # update base edges
                            baseEdge1 = baseEdge2
                            baseEdge2 = baseEdge

                            decompressT.append(node)
                            break
        else:
            baseEdge = baseEdge2
            all_nodes = roadnetwork.neighbors(baseEdge[1])
            direction = int(bitArray[i:i + 3], 2)
            i += 3
            for node in all_nodes:
                if node != baseEdge[0]:
                    d = numDirection(roadnetwork, vertice_dict, baseEdge[0], baseEdge[1], baseEdge[1], node)
                    if d == direction:
                        baseEdge = [baseEdge[1], node]
                        baseEdge1 = baseEdge2
                        baseEdge2 = baseEdge

                        decompressT.append(node)
                        break
    return decompressT


def checkCorrect(Origin, recover):
    for o, r in zip(Origin, recover):
        assert o == r


if __name__ == '__main__':
    maxTurn = 8
    Q = math.ceil(math.log(maxTurn))
    Sdm = '1' * (Q + 1)

    trajPath = 'C:\\Users\\HeAlec\\Desktop\\tdrive\\trajectory\\'
    mapMatchOutPath = 'C:\\Users\\HeAlec\\Desktop\\tdrive\\mapOut\\'
    nx_vertice, nx_edge, vertice_dict, edge_dict, edge_dist, edge_dist_dict, roadnetwork = network_data()

    i = 0
    for filename in os.listdir(mapMatchOutPath):
        i += 1
        mapTrajData = mapMatchOutPath + filename

        with open(mapTrajData, "r") as f:
            nodes = f.readline().strip("\n").split(",")
            nodes = [int(it) for it in nodes]
            print(nodes)

        excT_, cTo_ = NaiveClockWise(nodes)
        print(cTo_)
        excT, cTo = DORClockWise(nodes)
        print(cTo)

        print((len(nodes) * 32 - len(cTo)) / (len(nodes) * 32))
        print((len(nodes) * 32 - len(cTo_)) / (len(nodes) * 32))
        print((len(cTo_) - len(cTo)) / len(cTo_))
        dTo = DORClockWiseDecompress(excT, cTo)
        dTo_ = NaiveClockWiseDecompress(excT_, cTo_)
        # checkCorrect(nodes, dTo_)
        # checkCorrect(nodes, dTo)

        break
