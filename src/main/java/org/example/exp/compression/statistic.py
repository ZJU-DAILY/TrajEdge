import os

import numpy as np

from src.main.java.org.example.bolt.compression.utils import numDirection, network_data

if __name__ == '__main__':
    # trajId = 2187
    trajPath = 'C:\\Users\\HeAlec\\Desktop\\tdrive\\trajectory\\'
    mapMatchOutPath = 'C:\\Users\\HeAlec\\Desktop\\tdrive\\mapOut\\'
    nx_vertice, nx_edge, vertice_dict, edge_dict, edge_dist, edge_dist_dict, roadnetwork = network_data()

    i = 0
    avg = []
    avg1 = []
    for filename in os.listdir(mapMatchOutPath):
        # print(filename)
        i += 1
        mapTrajData = mapMatchOutPath + filename

        cnt = 0
        cnt1 = 0
        with open(mapTrajData, "r") as f:
            nodes = f.readline().strip("\n").split(",")
            nodes = [int(it) for it in nodes]
            # print(nodes)
            for i in range(2, len(nodes) - 1):
                d = numDirection(roadnetwork, vertice_dict, nodes[i - 2], nodes[i - 1], nodes[i], nodes[i + 1])
                if d == 0:
                    cnt += 1
                    continue
                if i >= 3:
                    d = numDirection(roadnetwork, vertice_dict, nodes[i - 3], nodes[i - 2], nodes[i], nodes[i + 1])
                    if d == 0:
                        cnt1 += 1
        avg.append(cnt)
        avg1.append(cnt1)

    print(np.mean(avg))
    print(np.mean(avg1))
