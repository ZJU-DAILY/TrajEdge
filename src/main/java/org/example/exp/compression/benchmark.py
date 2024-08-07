# https://github.com/rottenivy
# This Repository is under Apache License. See LICENSE file for full license text.
import math
import os
import time

from src.main.java.org.example.bolt.compression.utils import network_data, numDirection


def toBit(num: int):
    return "{0:0>{1}}".format(int(bin(num)[2:]), Q)


def NaiveClockWise(node_path):
    oriMethod = ''
    start = node_path[0]
    middle = node_path[1]
    startTime = time.time()
    for i in range(2, len(node_path)):
        end = node_path[i]
        d = numDirection(roadnetwork, vertice_dict, start, middle, middle, end)
        oriMethod += toBit(d)
        start = middle
        middle = end
    endTime = time.time()
    ot = endTime - startTime
    return oriMethod


def DORClockWise(node_path):
    D, F, To = '', '', ''
    gLength = 0
    for i in range(3, len(node_path) - 1):
        d0 = numDirection(roadnetwork, vertice_dict, nodes[i - 2], nodes[i - 1], nodes[i], nodes[i + 1])
        d1 = numDirection(roadnetwork, vertice_dict, nodes[i - 3], nodes[i - 2], nodes[i], nodes[i + 1])

        if d1 & d0 == 0:
            delta = '0' if d0 == 0 else '1'
            D += delta
            F += toBit(d0)
            gLength += 1
        else:
            if gLength + math.log(Q) + 1 < gLength * math.log(Q):
                tmp = Sdm + D
                To += tmp
            else:
                To += F
            D, F = '', ''
            gLength = 0
    return To


if __name__ == '__main__':
    Q = 3
    Sdm = '1111'

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

        cTo = DORClockWise(nodes)
        cTo_ = NaiveClockWise(nodes)
        print(cTo)
        print(cTo_)
