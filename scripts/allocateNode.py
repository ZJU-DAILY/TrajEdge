import random
import math
import subprocess
from collections import deque

# 经纬度范围S
S_LAT_MIN = 52.9
S_LAT_MAX = 53.6
S_LON_MIN = 7.7
S_LON_MAX = 8.3

# h的值
h = 4
n = 30

# 生成30个随机经纬度值
points = [(random.uniform(S_LAT_MIN, S_LAT_MAX), random.uniform(S_LON_MIN, S_LON_MAX)) for _ in range(n)]
print(points)

def find_center(lat_min, lat_max, lon_min, lon_max):
    return ((lat_min + lat_max) / 2, (lon_min + lon_max) / 2)

def calc_distance(lat1, lon1, lat2, lon2):
    # 地球半径，单位：米
    R = 6371000
    # 将角度转换为弧度
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    # 计算距离
    dLat = lat2_rad - lat1_rad
    dLon = lon2_rad - lon1_rad
    a = math.sin(dLat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dLon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c
    return distance

def find_nearest(center, points):
    min_distance = float('inf')
    nearest_point = None
    for point in points:
        distance = calc_distance(center[0], center[1], point[0], point[1])
        if distance < min_distance:
            min_distance = distance
            nearest_point = point
    return nearest_point

class Node:
    def __init__(self, prefix, lat_min, lat_max, lon_min, lon_max):
        self.prefix = prefix
        self.lat_min = lat_min
        self.lat_max = lat_max
        self.lon_min = lon_min
        self.lon_max = lon_max
        self.nearest_point = None
        self.children = []
        self.parent = None
        self.left_neighbor = None
        self.right_neighbor = None

def build_tree(prefix, lat_min, lat_max, lon_min, lon_max, depth, points, parent=None):
    node = Node(prefix, lat_min, lat_max, lon_min, lon_max)
    node.parent = parent
    
    if depth == 0:
        return node
    
    center = find_center(lat_min, lat_max, lon_min, lon_max)
    node.nearest_point = find_nearest(center, points)
    
    if depth > 0:
        mid_lat = (lat_min + lat_max) / 2
        mid_lon = (lon_min + lon_max) / 2
        
        # 创建四个子节点
        lu = build_tree(prefix + "0", lat_min, mid_lat, lon_min, mid_lon, depth - 1, points, node)
        ru = build_tree(prefix + "1", mid_lat, lat_max, lon_min, mid_lon, depth - 1, points, node)
        ld = build_tree(prefix + "2", lat_min, mid_lat, mid_lon, lon_max, depth - 1, points, node)
        rd = build_tree(prefix + "3", mid_lat, lat_max, mid_lon, lon_max, depth - 1, points, node)
        
        # 添加到children列表
        node.children = [lu, ru, ld, rd]
        
        # 设置同层节点之间的左右邻居关系
        lu.right_neighbor = ru
        ru.left_neighbor = lu
        
        ld.right_neighbor = rd
        rd.left_neighbor = ld
        
        # 设置上下层之间的左右邻居关系
        lu.right_neighbor = ld
        ld.left_neighbor = lu
        
        ru.right_neighbor = rd
        rd.left_neighbor = ru
    
    return node

def get_neighbors(node):
    """获取节点的所有邻居节点"""
    neighbors = []
    
    # 添加直接的左右邻居
    if node.left_neighbor:
        neighbors.append(node.left_neighbor)
    if node.right_neighbor:
        neighbors.append(node.right_neighbor)
    
    # # 通过父节点寻找其他邻居
    # if node.parent:
    #     parent_idx = node.parent.children.index(node)
        
    #     # 获取对角线方向的邻居
    #     if parent_idx == 0:  # 左上角
    #         if len(node.parent.children) > 3:
    #             neighbors.append(node.parent.children[3])  # 右下角
    #     elif parent_idx == 3:  # 右下角
    #         neighbors.append(node.parent.children[0])  # 左上角
    #     elif parent_idx == 1:  # 右上角
    #         if len(node.parent.children) > 2:
    #             neighbors.append(node.parent.children[2])  # 左下角
    #     elif parent_idx == 2:  # 左下角
    #         neighbors.append(node.parent.children[1])  # 右上角
    
    return neighbors

def write_allocations(root, points):
    # 清空所有现有的allocate文件
    for i in range(1, len(points) + 1):
        with open(f'../conf/allocate_{i}.txt', 'w') as file:
            pass
        with open(f'../conf/children_{i}.txt', 'w') as file:
            pass
        with open(f'../conf/parent_{i}.txt', 'w') as file:
            pass
        with open(f'../conf/neighbor_{i}.txt', 'w') as file:
            pass
    
    # 使用队列进行层序遍历
    queue = deque([root])
    while queue:
        node = queue.popleft()  # 修正了之前的 queue.queue.popleft()
        if node.nearest_point is not None:
            nodeId = points.index(node.nearest_point) + 1
            
            # 获取邻居节点信息
            neighbors = get_neighbors(node)
            neighbor_info = [f"{node.prefix}"]
            for neighbor in neighbors:
                if neighbor.nearest_point:
                    neighbor_id = points.index(neighbor.nearest_point) + 1
                    neighbor_info.append(f"{neighbor.prefix}:{neighbor_id}")
            
            # 写入节点信息和邻居信息
            with open(f'../conf/allocate_{nodeId}.txt', 'a') as file:
                file.write(f"{node.prefix}\n")
            if neighbor_info:
                 with open(f'../conf/neighbor_{nodeId}.txt', 'a') as file:
                    file.write(",".join(neighbor_info) + "\n")
            if node.parent:
                with open(f'../conf/parent_{nodeId}.txt', 'a') as file:
                    parent_id = points.index(node.parent.nearest_point) + 1
                    file.write(f"{node.prefix}, {node.parent.prefix}:{parent_id}" + "\n")
            # 孩子节点写入
            children_info = [ f"{child.prefix}:{points.index(child.nearest_point) + 1}" for child in node.children if child.nearest_point != None]
            if children_info:
                with open(f'../conf/children_{nodeId}.txt', 'a') as file:
                    file.write(f"{node.prefix}," + ",".join(children_info) + "\n")
        
        queue.extend(node.children)
    
    # 复制文件到Docker容器
    for i in range(1, n + 1):
        container_name = f'supervisor-{i}'
        mkdir_conf = f"docker exec {container_name} mkdir -p /data/conf"
        subprocess.run(mkdir_conf, shell=True)
        cp_to_docker = f"docker cp ../conf/allocate_{i}.txt {container_name}:/data/conf"
        subprocess.run(cp_to_docker, shell=True)
        cp_to_docker = f"docker cp ../conf/parent_{i}.txt {container_name}:/data/conf"
        subprocess.run(cp_to_docker, shell=True)
        cp_to_docker = f"docker cp ../conf/children_{i}.txt {container_name}:/data/conf"
        subprocess.run(cp_to_docker, shell=True)
        cp_to_docker = f"docker cp ../conf/neighbor_{i}.txt {container_name}:/data/conf"
        subprocess.run(cp_to_docker, shell=True)

# 修改主程序逻辑
root = build_tree("#", S_LAT_MIN, S_LAT_MAX, S_LON_MIN, S_LON_MAX, h + 1, points)
write_allocations(root, points)
