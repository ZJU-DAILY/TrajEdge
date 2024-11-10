import os
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import threading

x_min, x_max = 281, 23854  # 平面坐标系 X 范围
y_min, y_max = 3935, 30851  # 平面坐标系 Y 范围
lon_min, lon_max = 7.8, 8.2  # 经度范围
lat_min, lat_max = 53, 53.5  # 纬度范围

def local_to_latlon(x, y, x_min, x_max, y_min, y_max, lon_min, lon_max, lat_min, lat_max):
    """
    将平面坐标 (x, y) 转换为经纬度 (longitude, latitude)
    """
    longitude = lon_min + (x - x_min) / (x_max - x_min) * (lon_max - lon_min)
    latitude = lat_min + (y - y_min) / (y_max - y_min) * (lat_max - lat_min)
    return longitude, latitude

def process_line(line, index, output_dir):
    """
    处理单行数据
    """
    try:
        line = line.strip("\n")
        parts = line.split("\t")
        _id = parts[1]
        x, y = float(parts[4]), float(parts[5])
        tm = parts[6]
        longitude, latitude = local_to_latlon(x, y, x_min, x_max, y_min, y_max, lon_min, lon_max, lat_min, lat_max)
        
        # 使用线程锁确保文件写入的原子性
        output_file = os.path.join(output_dir, f"{index}_{_id}")
        with threading.Lock():
            with open(output_file, "a") as wf:
                wf.write(" ".join([str(latitude), str(longitude), tm]) + "\n")
        return True
    except Exception as e:
        print(f"Error processing line: {e}")
        return False

def process_file(filepath, index, output_dir):
    """
    处理单个文件
    """
    print(f"Processing {filepath}...")
    success_count = 0
    error_count = 0
    
    with open(filepath, "r") as f:
        lines = f.readlines()
        
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(process_line, line, index, output_dir) 
                  for line in lines]
        
        for future in tqdm(futures, desc=f"Processing {os.path.basename(filepath)}"):
            if future.result():
                success_count += 1
            else:
                error_count += 1
    
    print(f"File {filepath} completed. Success: {success_count}, Errors: {error_count}")

def main():
    input_path = "/home/hch/PROJECT/data/oldenburg/raw"
    output_dir = "/home/hch/PROJECT/data/oldenburg/trajectory"
    exclude = ['o6.dat', 'o5.dat', 'o13.dat', 'o7.dat']
    
    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)
    
    for index in range(7, 13):
        filename = f"o{index}.dat"
        if filename in exclude:
            continue
        
        filepath = os.path.join(input_path, filename)
        process_file(filepath, index, output_dir)

if __name__ == "__main__":
    main()
