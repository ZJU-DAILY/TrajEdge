import random
import math
import subprocess

# 经纬度范围S
S_LAT_MIN = 35.0
S_LAT_MAX = 40.0
S_LON_MIN = -120.0
S_LON_MAX = -115.0

# h的值
h = 3
n = 3

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

def divide_and_assign(lat_min, lat_max, lon_min, lon_max, depth, points):
    if depth == 0:
        return
    
    center = find_center(lat_min, lat_max, lon_min, lon_max)
    nearest_point = find_nearest(center, points)
    # print(f"[ {lat_min},{lon_min},{lat_max},{lon_max} ]分配给最近的点：{points.index(nearest_point)}")
    # docker_cmd = f"docker exec supervisor-{points.index(nearest_point)} bash -c 'export treeNode=x'"
    docker_cmd = f"docker exec supervisor-{points.index(nearest_point)} bash -c" + 'echo $CONTAINER_ID'
    subprocess.run(docker_cmd, shell=True)
    
    if depth > 0:
        mid_lat = (lat_min + lat_max) / 2
        mid_lon = (lon_min + lon_max) / 2
        
        # 左上
        divide_and_assign(lat_min, mid_lat, lon_min, mid_lon, depth - 1, points)
        # 右上
        divide_and_assign(mid_lat, lat_max, lon_min, mid_lon, depth - 1, points)
        # 左下
        divide_and_assign(lat_min, mid_lat, mid_lon, lon_max, depth - 1, points)
        # 右下
        divide_and_assign(mid_lat, lat_max, mid_lon, lon_max, depth - 1, points)

# 开始递归分配
divide_and_assign(S_LAT_MIN, S_LAT_MAX, S_LON_MIN, S_LON_MAX, h, points)
