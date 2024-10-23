# -*- coding: utf-8 -*-
# @Time    : 2023/11/23 15:58
# @Author  : HeAlec
# @FileName: main.py
# @Desc: description
# @blogs ：https://segmentfault.com/u/alec_5e8962e4635ca

import pandas as pd

from leuvenmapmatching.map.inmem import InMemMap
from leuvenmapmatching.matcher.distance import DistanceMatcher


class CompressBolt(BasicBolt):

    def __init__(self, base):
        nx_vertice = pd.read_csv(base + 'roadmap/node.csv',
                                 usecols=['node', 'lng', 'lat'])

        rdnetwork = pd.read_csv(base + 'roadmap/edge_weight.csv',
                                usecols=['section_id', 's_node', 'e_node', 'length'])

        map_con = InMemMap("myosm", use_latlon=True, use_rtree=True, index_edges=True)

        for row in nx_vertice.values:
            map_con.add_node(int(row[0]), (float(row[2]), float(row[1])))

        for row in rdnetwork.values:
            map_con.add_edge(int(row[1]), int(row[2]))

        self.trajectory = []

    def process(self, point):
        self.trajectory.append(point)
        res = self.network_data()
        return res

    def data_convert(self, taxigps_day):
        def thread_task(df):
            return list(zip(df['LON'], df['LAT']))

        traj_task = pd.DataFrame(taxigps_day.groupby('TRAJ_ID').apply(thread_task), columns=['TRAJ_LIST'])
        traj_task.reset_index(level=['TRAJ_ID'], inplace=True)
        traj_task_list = []
        for row in traj_task.values:
            traj_task_list.append({row[0]: row[1]})
        return traj_task_list

    def to_csv(self, trajData, trajId):
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

    def network_data(self):

        track = []
        for t in self.trajectory.to_numpy().tolist():
            track.append((float(t[1]), float(t[0])))

        matcher = DistanceMatcher(self.map_con,
                                  max_dist=1000, max_dist_init=50,  # meter
                                  non_emitting_length_factor=0.75,
                                  obs_noise=50, obs_noise_ne=75,  # meter
                                  dist_noise=50,  # meter
                                  non_emitting_states=True,
                                  max_lattice_width=5)
        states, lastidx = matcher.match(track)
        return states[-1][1]
