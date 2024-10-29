import os

# 初始化最小最大值
min_lat, max_lat = float('inf'), float('-inf')
min_lng, max_lng = float('inf'), float('-inf')
min_timestamp, max_timestamp = float('inf'), float('-inf')

# 遍历目录下的所有文件
directory = '/home/hch/PROJECT/data/brinkhoff/trajectory'
for filename in os.listdir(directory):
    # 构建完整的文件路径
    filepath = os.path.join(directory, filename)
    
    # 检查是否为文件
    if os.path.isfile(filepath):
        with open(filepath, 'r') as file:
            for line in file:
                # 读取数据并分割
                parts = line.strip().split()
                if len(parts) < 3:
                    continue  # 跳过不完整的行
                
                # 转换数据类型
                lat, lng, timestamp = float(parts[0]), float(parts[1]), float(parts[2])
                if lat > 90 or lat < -90 or lng > 180 or lng < -180:
                    print(file)
                    break
                
                # 更新最小最大值
                if lat < min_lat:
                    min_lat = lat
                if lat > max_lat:
                    max_lat = lat
                if lng < min_lng:
                    min_lng = lng
                if lng > max_lng:
                    max_lng = lng
                if timestamp < min_timestamp:
                    min_timestamp = timestamp
                if timestamp > max_timestamp:
                    max_timestamp = timestamp

# 打印结果
print(f"最小纬度: {min_lat}, 最大纬度: {max_lat}")
print(f"最小经度: {min_lng}, 最大经度: {max_lng}")
print(f"最小时间戳: {min_timestamp}, 最大时间戳: {max_timestamp}")