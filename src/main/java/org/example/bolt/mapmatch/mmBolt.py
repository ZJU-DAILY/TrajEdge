# -*- coding: utf-8 -*-
# @Time    : 2023/11/23 15:58
# @Author  : HeAlec
# @FileName: main.py
# @Desc: description
# @blogs ：https://segmentfault.com/u/alec_5e8962e4635ca
import os

import pandas as pd
from leuvenmapmatching.map.inmem import InMemMap


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

    df = pd.DataFrame(data)
    return df


def to_trajectory(trajId):
    trajData = trajPath + str(trajId)
    trajectory = to_csv(trajData, trajId)
    #  lat经度 lng纬度
    trajectory = data_convert(trajectory)

    perTraj = trajectory[0]

    traj_list = list(perTraj.values())[0]
    traj = pd.DataFrame(traj_list, columns=['LON', 'LAT'])
    # 因为可能存在重复的gps点，所以此处直接dedup一下
    traj.drop_duplicates(['LON', 'LAT'], inplace=True)
    return traj


def network_data(trajId):
    from leuvenmapmatching.matcher.distance import DistanceMatcher

    traj = to_trajectory(trajId)
    track = []
    for t in traj.to_numpy().tolist():
        track.append((float(t[1]), float(t[0])))

    matcher = DistanceMatcher(map_con,
                              max_dist=1000, max_dist_init=50,  # meter
                              non_emitting_length_factor=0.75,
                              obs_noise=50, obs_noise_ne=75,  # meter
                              dist_noise=50,  # meter
                              non_emitting_states=True,
                              max_lattice_width=5)
    states, lastidx = matcher.match(track)

    res = [str(it[0]) for it in states]
    res.append(str(states[-1][1]))
    resString = ",".join(res)
    # print(resString)
    return resString


if __name__ == '__main__':
    base = '/home/hch/PROJECT/data/tdrive/'
    trajPath = '/home/hch/PROJECT/data/tdrive/trajectory/'
    mapMatchOutPath = '/home/hch/PROJECT/data/tdrive/mapOut/'

    nx_vertice = pd.read_csv(base + 'roadmap/node.csv',
                             usecols=['node', 'lng', 'lat'])

    rdnetwork = pd.read_csv(base + 'roadmap/edge_weight.csv',
                            usecols=['section_id', 's_node', 'e_node', 'length'])

    map_con = InMemMap("myosm", use_latlon=True, use_rtree=True, index_edges=True)

    for row in nx_vertice.values:
        map_con.add_node(int(row[0]), (float(row[2]), float(row[1])))

    for row in rdnetwork.values:
        map_con.add_edge(int(row[1]), int(row[2]))
        # map_con.add_edge(int(row[2]), int(row[1]))

    errNum = 0
    total = 0
    for filename in os.listdir(trajPath):
        print(filename)
        total += 1
        try:
            res = network_data(int(filename))
            with open(os.path.join(mapMatchOutPath, filename), "w") as f:
                f.write(res)
        except Exception as e:
            errNum += 1
            print(filename + "error")
            print(e)
    # network_data()
    print("correct ratio: {}".format((total - errNum) / total))
