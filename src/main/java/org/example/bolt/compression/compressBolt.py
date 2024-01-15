# https://github.com/rottenivy
# This Repository is under Apache License. See LICENSE file for full license text.
import csv
import math
import os
import time

import numpy as np
import pandas as pd
from itertools import tee
import networkx as nx
from operator import itemgetter
import matplotlib.pyplot as plt

pd.options.mode.chained_assignment = None
np.seterr(divide='ignore')

# 100m grid
loninter = 0.000976
latinter = 0.0009


def haversine(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    c = 2 * np.arcsin(np.sqrt(a))
    return c * 6367 * 1000


def pairwise(iterable):
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)


def network_data():
    base = '/home/hch/PROJECT/data/tdrive/'
    nx_vertice = pd.read_csv(base + 'roadmap/node.csv',
                             usecols=['node', 'lng', 'lat'])
    vertice_dict = nx_vertice.set_index('node').T.to_dict('list')

    nx_edge = pd.read_csv(base + 'roadmap/edge.csv',
                          usecols=['edge', 's_node', 'e_node', 's_lng', 's_lat', 'e_lng', 'e_lat', 'c_lng', 'c_lat'])
    edge_dict = nx_edge.set_index('edge').T.to_dict('list')

    rdnetwork = pd.read_csv(base + 'roadmap/edge_weight.csv',
                            usecols=['section_id', 's_node', 'e_node', 'length'])

    edge_dist = rdnetwork[['s_node', 'e_node', 'length', 'section_id']]
    edge_dist['idx'] = list(zip(edge_dist['s_node'], edge_dist['e_node']))
    edge_dist = edge_dist[['idx', 'length', 'section_id']]
    edge_dist_dict = edge_dist.set_index('idx').T.to_dict('list')
    edge_dist = edge_dist[['section_id', 'length']].set_index('section_id')['length'].to_dict()

    roadnetwork = nx.DiGraph()
    for row in rdnetwork.values:
        roadnetwork.add_edge(int(row[1]), int(row[2]), distance=row[-1])
        roadnetwork.add_edge(int(row[2]), int(row[1]), distance=row[-1])

    return nx_vertice, nx_edge, vertice_dict, edge_dict, edge_dist, edge_dist_dict, roadnetwork


def get_traj2edge_distance(traj_point, sub_nx_edge):
    sub_nx_edge['a'] = haversine(traj_point[0], traj_point[1], sub_nx_edge['s_lng'], sub_nx_edge['s_lat'])
    sub_nx_edge['b'] = haversine(traj_point[0], traj_point[1], sub_nx_edge['e_lng'], sub_nx_edge['e_lat'])
    sub_nx_edge['c'] = haversine(sub_nx_edge['s_lng'], sub_nx_edge['s_lat'], sub_nx_edge['e_lng'], sub_nx_edge['e_lat'])
    indexer1 = sub_nx_edge['b'] ** 2 > sub_nx_edge['a'] ** 2 + sub_nx_edge['c'] ** 2
    indexer2 = sub_nx_edge['a'] ** 2 > sub_nx_edge['b'] ** 2 + sub_nx_edge['c'] ** 2
    sub_nx_edge.loc[indexer1, 'shortest_dist'] = sub_nx_edge.loc[indexer1, 'a']
    sub_nx_edge.loc[indexer1, 'matched_nd'] = sub_nx_edge.loc[indexer1, 's_node']
    sub_nx_edge.loc[indexer2, 'shortest_dist'] = sub_nx_edge.loc[indexer2, 'b']
    sub_nx_edge.loc[indexer2, 'matched_nd'] = sub_nx_edge.loc[indexer2, 'e_node']

    sub_nx_edge['l'] = (sub_nx_edge['a'] + sub_nx_edge['b'] + sub_nx_edge['c']) / 2
    sub_nx_edge['s'] = np.sqrt(sub_nx_edge['l'] * np.abs(sub_nx_edge['l'] - sub_nx_edge['a']) * np.abs(
        sub_nx_edge['l'] - sub_nx_edge['b']) * np.abs(sub_nx_edge['l'] - sub_nx_edge['c']))

    indexer3 = pd.isnull(sub_nx_edge['shortest_dist'])
    sub_nx_edge.loc[indexer3, 'shortest_dist'] = 2 * sub_nx_edge.loc[indexer3, 's'] / sub_nx_edge.loc[indexer3, 'c']

    return sub_nx_edge[['edge', 'shortest_dist', 'matched_nd']]


def get_candidates(row):
    traj_point = [row['LON'], row['LAT']]
    sub_nx_edge = nx_edge[((nx_edge['s_lng'] >= traj_point[0] - loninter) & (
            nx_edge['s_lng'] <= traj_point[0] + loninter) & (nx_edge['s_lat'] >= traj_point[1] - latinter) & (
                                   nx_edge['s_lat'] <= traj_point[1] + latinter)) | (
                                  (nx_edge['e_lng'] >= traj_point[0] - loninter) & (
                                  nx_edge['e_lng'] <= traj_point[0] + loninter) & (
                                          nx_edge['e_lat'] >= traj_point[1] - latinter) & (
                                          nx_edge['e_lat'] <= traj_point[1] + latinter)) | (
                                  (nx_edge['c_lng'] >= traj_point[0] - loninter) & (
                                  nx_edge['c_lng'] <= traj_point[0] + loninter) & (
                                          nx_edge['c_lat'] >= traj_point[1] - latinter) & (
                                          nx_edge['c_lat'] <= traj_point[1] + latinter))]
    cand_edges = get_traj2edge_distance(traj_point, sub_nx_edge)
    cand_edges = cand_edges[(cand_edges['shortest_dist'] <= 35) & pd.notnull(cand_edges['shortest_dist'])]
    cand_edges['shortest_dist'] = round(cand_edges['shortest_dist'])
    if not cand_edges.empty:
        return cand_edges['edge'].tolist(), cand_edges['matched_nd'].tolist(), cand_edges['shortest_dist'].tolist()
    else:
        return -1, -1, -1


def observation_probability(row):
    cand_nd_df = np.array(row['CAND_ND_DIS'])
    cand_nd_df = 1 / (np.sqrt(2 * np.pi) * 20) * np.exp(-cand_nd_df ** 2 / 800)
    return list(cand_nd_df)


def transmission_probability(traj):
    v_list = [[]]
    for row1, row2 in pairwise(traj.values):
        d = haversine(row1[0], row1[1], row2[0], row2[1])
        row_v_list = []
        for idx1, nd1 in enumerate(row1[-2]):
            temp_list = []
            for idx2, nd2 in enumerate(row2[-2]):
                try:  # nd1 and nd2 are not connected
                    if pd.notnull(nd1) and pd.notnull(nd2):
                        temp_list.append(d / nx.astar_path_length(roadnetwork, nd1, nd2, weight='distance'))
                    elif pd.notnull(nd1):
                        nd2_back_node = edge_dict[row2[-3][idx2]][0]
                        nd2_back_node_cor = vertice_dict[nd2_back_node]
                        temp_list.append(d / (
                                nx.astar_path_length(roadnetwork, nd1, nd2_back_node, weight='distance') + np.sqrt(
                            np.abs(haversine(row2[0], row2[1], nd2_back_node_cor[0], nd2_back_node_cor[1]) ** 2 -
                                   row2[-4][idx2] ** 2))))
                    elif pd.notnull(nd2):
                        nd1_forward_node = edge_dict[row1[-3][idx1]][1]
                        nd1_forward_node_cor = vertice_dict[nd1_forward_node]
                        temp_list.append(d / (nx.astar_path_length(roadnetwork, nd1_forward_node, nd2,
                                                                   weight='distance') + np.sqrt(np.abs(
                            haversine(row1[0], row1[1], nd1_forward_node_cor[0], nd1_forward_node_cor[1]) ** 2 -
                            row1[-4][idx1] ** 2))))
                    else:
                        nd1_forward_node = edge_dict[row1[-3][idx1]][1]
                        nd1_forward_node_cor = vertice_dict[nd1_forward_node]
                        nd2_back_node = edge_dict[row2[-3][idx2]][0]
                        nd2_back_node_cor = vertice_dict[nd2_back_node]
                        temp_list.append(d / (nx.astar_path_length(roadnetwork, nd1_forward_node, nd2_back_node,
                                                                   weight='distance') + np.sqrt(np.abs(
                            haversine(row1[0], row1[1], nd1_forward_node_cor[0], nd1_forward_node_cor[1]) ** 2 -
                            row1[-4][idx1] ** 2)) + np.sqrt(np.abs(
                            haversine(row2[0], row2[1], nd2_back_node_cor[0], nd2_back_node_cor[1]) ** 2 - row2[-4][
                                idx2] ** 2))))
                except:
                    temp_list.append(0)
            row_v_list.append(temp_list)
        v_list.append(row_v_list)
    return v_list


def spatial_analysis(row):
    return [[n_i * v_i[i] if not np.isinf(v_i[i]) else n_i for v_i in row[-1]] for i, n_i in enumerate(row[-2])]


def candidate_graph(traj):
    max_f = max([max([max(f) for f in f_list]) for f_list in traj['F'].tolist()[1:]])
    cand_graph = nx.DiGraph()
    idx = 0
    for row1, row2 in pairwise(traj.values):
        for i, nd2 in enumerate(row2[-5]):
            for j, nd1 in enumerate(row1[-5]):
                cand_graph.add_edge(str(idx) + '-' + str(nd1), str(idx + 1) + '-' + str(nd2),
                                    distance=max_f - row2[-1][i][j])
        idx += 1
    return cand_graph


def trajectory_matching(traj):
    traj_id = list(traj.keys())[0]
    traj_list = list(traj.values())[0]
    traj = pd.DataFrame(traj_list, columns=['LON', 'LAT'])
    # 因为可能存在重复的gps点，所以此处直接dedup一下
    traj.drop_duplicates(['LON', 'LAT'], inplace=True)
    print(traj.shape)
    results = traj.apply(get_candidates, axis=1)
    traj['CAND_ND_DIS'] = [x[2] if x != -1 else -1 for x in results]
    traj['CAND_EG'] = [x[0] if x != -1 else -1 for x in results]
    traj['CAND_ND'] = [x[1] if x != -1 else -1 for x in results]
    traj = traj[traj['CAND_EG'] != -1]
    if traj.shape[0] > 1:  # not enough candidates
        traj['N'] = traj.apply(observation_probability, axis=1)
        traj['V'] = transmission_probability(traj)
        traj['F'] = traj.apply(spatial_analysis, axis=1)
        cand_graph = candidate_graph(traj)
        try:
            cand_path_dict = {
                nx.shortest_path_length(cand_graph, '0-' + str(s_node), str(traj.shape[0] - 1) + '-' + str(e_node),
                                        weight='distance'): nx.shortest_path(cand_graph, '0-' + str(s_node),
                                                                             str(traj.shape[0] - 1) + '-' + str(e_node),
                                                                             weight='distance') for e_node in
                traj.iloc[-1]['CAND_EG'] for s_node in traj.iloc[0]['CAND_EG']}
        except:
            return pd.DataFrame([[traj_id, -1, -1]], columns=['TRAJ_ID', 'MATCHED_EDGE', 'MATCHED_NODE'])
        matched_path = min(cand_path_dict.items(), key=itemgetter(0))[1]
        matched_path = [int(x[x.index('-') + 1:]) for x in matched_path]
        cand_node_list = traj['CAND_ND'].tolist()
        cand_edge_list = traj['CAND_EG'].tolist()
        matched_node = [cand_node_list[idx][cand_edge_list[idx].index(me)] for idx, me in enumerate(matched_path)]
        return pd.DataFrame([[traj_id, matched_path, matched_node]],
                            columns=['TRAJ_ID', 'MATCHED_EDGE', 'MATCHED_NODE'])
    else:
        return pd.DataFrame([[traj_id, -1, -1]], columns=['TRAJ_ID', 'MATCHED_EDGE', 'MATCHED_NODE'])


def data_convert(taxigps_day):
    def thread_task(df):
        return list(zip(df['LON'], df['LAT']))

    traj_task = pd.DataFrame(taxigps_day.groupby('TRAJ_ID').apply(thread_task), columns=['TRAJ_LIST'])
    traj_task.reset_index(level=['TRAJ_ID'], inplace=True)
    traj_task_list = []
    for row in traj_task.values:
        traj_task_list.append({row[0]: row[1]})
    return traj_task_list


def to_csv(trajData, trajId):
    data = {
        'TRAJ_ID': [],
        'TIME': [],
        'LON': [],
        'LAT': []
    }
    with open(trajData, 'r') as f:
        for i, point in enumerate(f):
            point = str(point).strip("\n").split(" ")
            data['TRAJ_ID'].append(trajId)
            data['TIME'].append(point[2])
            data['LAT'].append(point[0])
            data['LON'].append(point[1])
            if i > 30:
                break

    df = pd.DataFrame(data)

    # 将DataFrame写入CSV文件，指定分隔符为制表符，并将缺失值表示为'NA'
    df.to_csv(trajData + ".csv", header=False, index=False)


def to_nodes(edgeList: list):
    res = []
    for edge in edgeList:
        res.append(edge_dict[edge][0])
        res.append(edge_dict[edge][1])
    return res


def is_connected(nodeList: list):
    for i in range(1, len(nodeList)):
        assert roadnetwork.has_edge(nodeList[i - 1], nodeList[i]) or roadnetwork.has_edge(nodeList[i],
                                                                                          nodeList[i - 1]) or nodeList[
                   i] == nodeList[i - 1], \
            "node_{}: {} node_{}: {}".format(i, nodeList[i],
                                             i - 1,
                                             nodeList[i - 1])


def statistic():
    freq = {}
    for num in vertice_dict.keys():
        try:
            f = len(roadnetwork[num].keys())
        except:
            f = 0
        if f not in freq:
            freq[f] = 1
        else:
            freq[f] += 1
    for k, v in freq:
        print("出口数: {}, 出口频率: {}".format(k, v))


def CalAngle(v1, v2):
    # v1旋转到v2，逆时针为正，顺时针为负
    # 2个向量模的乘积
    TheNorm = np.linalg.norm(v1) * np.linalg.norm(v2)
    # 叉乘
    rho = np.rad2deg(np.arcsin(np.cross(v1, v2) / TheNorm))
    # 点乘
    theta = np.rad2deg(np.arccos(np.dot(v1, v2) / TheNorm))
    if rho < 0:
        return - theta
    else:
        return theta


def to_vector(p1, p2):
    return p2[0] - p1[0], p2[1] - p1[1]


def numDirection(stPath_st, stPath_end, middle, end):
    all_nodes = roadnetwork.neighbors(middle)
    middle_pos = vertice_dict[middle]
    stPathst_pos = vertice_dict[stPath_st]
    stPathend_pos = vertice_dict[stPath_end]
    res = []
    for node in all_nodes:
        node_pos = vertice_dict[node]
        angle = 180 + CalAngle(to_vector(stPathst_pos, stPathend_pos), to_vector(middle_pos, node_pos))
        res.append((node, angle))

    res.sort(key=lambda e: e[1])
    for i, id in enumerate(res):
        if (id[0] == end):
            return i
    # print(prev, cur)
    return -1


def clockwise(node_path):
    oriMethod = []
    newMethod = []
    newIdx = []
    start = node_path[0]
    middle = node_path[1]
    startTime = time.time()
    for i in range(2, len(node_path)):
        end = node_path[i]
        d = numDirection(start, middle, middle, end)
        oriMethod.append(d)
        start = middle
        middle = end
    endTime = time.time()
    ot = endTime - startTime

    start = node_path[0]
    middle = node_path[1]
    step = 1
    startTime = time.time()
    for i in range(2, len(node_path)):
        end = node_path[i]
        d = numDirection(start, middle, middle, end)
        if i - 1 <= step:
            newMethod.append(d)
            newIdx.append(0)
        else:
            min_ = d
            min_idx = 0
            for j in range(step):
                start_ = node_path[i - 2 - (j + 1)]
                middle_ = node_path[i - 1 - (j + 1)]
                d_d = numDirection(start_, middle_, middle, end)
                if dict[d_d + 1] < dict[min_ + 1]:
                    min_ = d_d
                    min_idx = j + 1
            newMethod.append(min_)
            newIdx.append(min_idx)
        start = middle
        middle = end
    endTime = time.time()
    et = endTime - startTime
    # print(oriMethod)
    # print(newIdx)
    # print(newMethod)
    return oriMethod, newIdx, newMethod, ot, et


def clockwise_group(node_path, st, en):
    # start = node_path[0]
    # middle = node_path[1]
    all_d = []
    all_dd = []
    en = min(en, len(node_path) - 1)
    credit = 0
    for i in range(st, en):
        start = node_path[i - 2]
        middle = node_path[i - 1]
        end = node_path[i]
        d = numDirection(start, middle, middle, end)
        all_d.append(d)

        start_ = node_path[i - 2 - 1]
        middle_ = node_path[i - 1 - 1]
        d_d = numDirection(start_, middle_, middle, end)
        all_dd.append(d_d)

        credit += dict[d + 1] - dict[d_d + 1]
    if credit > 1:
        return 1, all_dd
    return 0, all_d


def clockwise_v3(node_path):
    oriMethod = []
    newMethod = []
    newIdx = []
    start = node_path[0]
    middle = node_path[1]
    startTime = time.time()
    for i in range(2, len(node_path)):
        end = node_path[i]
        d = numDirection(start, middle, middle, end)
        oriMethod.append(d)
        start = middle
        middle = end
    endTime = time.time()
    ot = endTime - startTime

    start = node_path[0]
    middle = node_path[1]
    step = 7
    startTime = time.time()
    i = 2
    while i < len(node_path):
        if i - 1 <= step:
            end = node_path[i]
            d = numDirection(start, middle, middle, end)
            newMethod.append(d)
            start = middle
            middle = end
            if i - 1 == step:
                newIdx.append(0)
            i += 1
        else:
            choice, d_d = clockwise_group(node_path, i, i + step)
            newMethod.extend(d_d)
            newIdx.append(choice)
            i += step

    endTime = time.time()
    et = endTime - startTime
    # print(oriMethod)
    # print(newIdx)
    # print(newMethod)
    return oriMethod, newIdx, newMethod, ot, et




def clockwise_mining(node_path):
    res = []
    step = 1
    stIndex = 3
    enIndex = 0
    for i in range(stIndex, len(node_path)):
        start = node_path[i - 2]
        middle = node_path[i - 1]
        end = node_path[i]
        d = numDirection(start, middle, middle, end)
        if d == 0:
            enIndex = i
        else:
            for j in range(step):
                start_ = node_path[i - 2 - (j + 1)]
                middle_ = node_path[i - 1 - (j + 1)]
                d_d = numDirection(start_, middle_, middle, end)
                if d_d == 0:
                    enIndex = i
                else:
                    length = enIndex - stIndex
                    stIndex = i
                    enIndex = i
                    if length + 4 < 3 * length:
                        res.append(3 * length - length - 4)
    return res


def rle_decompress(s):
    result = ''
    count = ''
    for _ in s:
        if _.isdigit():
            count += _
        else:
            result += _ * int(count)
            count = ''
    return result


def rle_compress(s):
    """
    对字符串使用RLE算法压缩，s中不能出现数字
    :param s:
    :return:
    """
    result = ''
    last = s[0]
    count = 1
    for _ in s[1:]:
        if last == _:
            count += 1
        else:
            result += str(count) + last
            last = _
            count = 1
    result += str(count) + last
    return result


def dedup(source: list):
    res = [source[0]]
    for i in range(1, len(source)):
        if source[i] == res[-1]:
            continue
        res.append(source[i])
    return res


if __name__ == '__main__':
    dict = {
        2: 1,
        1: 2,
        3: 3,
        4: 4,
        5: 5,
        6: 6,
        7: 7,
        8: 7
    }
    # trajId = 2187
    trajPath = '/home/hch/PROJECT/data/tdrive/trajectory/'
    mapMatchOutPath = '/home/hch/PROJECT/data/tdrive/mapOut/'
    nx_vertice, nx_edge, vertice_dict, edge_dict, edge_dist, edge_dist_dict, roadnetwork = network_data()
    oldSum = 0
    newSum = 0
    enhanceSum = 0
    oriSum = 0
    oriTime = 0
    enhanceTime = 0

    i = 0
    for filename in os.listdir(mapMatchOutPath):
        # print(filename)
        i += 1
        mapTrajData = mapMatchOutPath + filename

        with open(mapTrajData, "r") as f:
            nodes = f.readline().strip("\n").split(",")
            nodes = [int(it) for it in nodes]
        nodes = dedup(nodes)
        # print(nodes)
        is_connected(nodes)
        res = clockwise_mining(nodes)
        print(np.sum(res))
        ori, index, new, ot, et = clockwise_v3(nodes)
        oriTime += ot
        enhanceTime += et

        oriSize = len(ori) * 3 / 8 + 4
        # nowSize = (len(index) + ((np.max(new) + 1) // 2 + 1) * len(new)) / 8
        nowSize = 0
        enhanceSize = 0
        for num in ori:
            nowSize += dict[num + 1]
        for num in new:
            enhanceSize += dict[num + 1]


        nowSize = nowSize / 8 + 4
        enhanceSize = enhanceSize / 8 + 4

        oldSum += oriSize
        newSum += nowSize
        oriSum += len(nodes) * 4
        enhanceSum += enhanceSize
        if i % 800 == 0:
            print("=========================================================")
            print((oriSum - oldSum) / oriSum, " ", (oriSum - newSum) / oriSum, " ", (oriSum - enhanceSum) / oriSum)
            print("ori: ", oriTime, "ehanceTime: ", enhanceTime)
            print("=========================================================")
