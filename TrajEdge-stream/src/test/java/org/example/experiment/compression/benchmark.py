# https://github.com/rottenivy
# This Repository is under Apache License. See LICENSE file for full license text.
import math
import os
import time
import zstandard as zstd
from utils import network_data, numDirection
import numpy as np
from tqdm import tqdm 

def toBit(num: int):
    # print(num)
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

# 压缩数据
def ZstdCompressor(data: bytes) -> bytes:
    # 创建压缩器
    cctx = zstd.ZstdCompressor()
    start_time = time.time()  # 记录开始时间
    # 压缩数据并返回
    compressed_data = cctx.compress(data)
    end_time = time.time()  # 记录结束时间
    compression_time = end_time - start_time  # 计算压缩时间
    return compressed_data, compression_time

def ZstdDecompressor(data: bytes) -> bytes:
    start_time = time.time()  # 记录解压开始时间
    decompressed_data = zstd.decompress(zstdRes)  # 解压缩数据
    end_time = time.time()  # 记录解压结束时间
    decompression_time = end_time - start_time  # 计算解压时间
    return decompressed_data, decompression_time

def compressRatio(size, prevSize):
    return (prevSize - size) / prevSize


if __name__ == '__main__':
    Q = 3
    Sdm = '1111'
    verbose = False

    trajPath = '/home/hch/PROJECT/data/tdrive/trajectory/'
    mapMatchOutPath = '/home/hch/PROJECT/data/tdrive/mapOut/'
    networkPath = '/home/hch/PROJECT/data/tdrive/'
    # nx_vertice, nx_edge, vertice_dict, edge_dict, edge_dist, edge_dist_dict, roadnetwork = network_data(networkPath)

    i = 0
    cRatio = []
    compressTimes = []
    decompressTimes = []
    rawSizes = []
    
    for filename in tqdm(os.listdir(mapMatchOutPath), desc="Processing files"):
        mapTrajData = os.path.join(mapMatchOutPath, filename)

        with open(mapTrajData, "r") as f:
            nodes = f.readline().strip("\n").split(",")
            nodes = [int(it) for it in nodes]
        
        with open(mapTrajData, "rb+") as f:
            raw = f.read()

        try:
            # cTo = DORClockWise(nodes)
            # cTo_ = NaiveClockWise(nodes)
            zstdRes, compressTime = ZstdCompressor(raw)
            revZstdRes, decompressionTime = ZstdDecompressor(zstdRes)
            
            # bit
            rawSize = len(nodes) * 32
            zstdSize = len(zstdRes) * 8

            if verbose:
                # dorSize = len(cTo)
                # ncSize = len(cTo_)
                print(compressRatio(dorSize, rawSize))
                print(compressRatio(ncSize, rawSize))
                print(compressRatio(zstdSize, rawSize))

                print((ncSize - dorSize) / ncSize)
                print((zstdSize - dorSize) / zstdSize)
            
            cRatio.append(compressRatio(zstdSize, rawSize))
            compressTimes.append(compressTime)
            decompressTimes.append(decompressionTime)
            rawSizes.append(rawSize)

            i += 1
            if i % 800 == 0:
                print(np.sum(rawSizes) / (2 * 1024 * 1024))
                print(np.mean(cRatio))
                print(np.mean(compressTimes))
                print(np.mean(decompressTimes))
        except Exception as e:
            pass
            # print(f"An error occurred: {e}")  # 打印异常信息
            
