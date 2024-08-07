import math

from storm import BasicBolt

from src.main.java.org.example.bolt.compression.utils import numDirection
from src.main.java.org.example.exp.compression.correctness import toBitWithWidth


class CompressBolt(BasicBolt):

    def __init__(self, maxTurn, initEdge1, initEdge2, roadnetwork, vertice_dict):
        self.baseEdge1 = initEdge1
        self.baseEdge2 = initEdge2
        self.vertice_dict = vertice_dict
        self.roadnetwork = roadnetwork
        self.D, self.F = '', ''
        self.gLength = 0
        self.maxTurn = maxTurn
        self.Q = math.ceil(math.log(maxTurn))
        self.Sdm = '1' * (self.Q + 1)

    def process(self, edge):
        sId = edge.values[0]
        eId = edge.values[1]
        d0 = numDirection(self.roadnetwork, self.vertice_dict, self.baseEdge1[0], self.baseEdge1[1], sId,
                          eId)
        d1 = numDirection(self.roadnetwork, self.vertice_dict, self.baseEdge2[0], self.baseEdge2[1], sId,
                          eId)

        res = None
        if d1 & d0 == 0:
            delta = '0' if d0 == 0 else '1'
            self.D += delta
            self.F += self.toBit(d0)
            self.gLength += 1
        else:
            if self.gLength + math.log(self.Q) + 1 < self.gLength * math.log(self.Q):
                res = self.Sdm + toBitWithWidth(self.gLength, 4) + self.D
            else:
                res = self.F
            self.D, self.F = '', ''
            self.gLength = 0
        return res

    def toBit(self, num: int):
        return "{0:0>{1}}".format(int(bin(num)[2:]), self.Q)
