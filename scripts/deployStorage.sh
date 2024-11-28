#!/bin/bash

# 定义起始和结束的整数
start=1
end=10

# 遍历从 $start 到 $end 的整数
for i in $(seq $start $end); do
  # 拼接容器名称
  container="supervisor-$i"
  
  echo "Copying TrajEdge-storage-1.0-SNAPSHOT.jar to $container"
  # 拷贝 .jar 文件到容器的 /opt/storm/lib 目录
  docker cp /home/hch/PROJECT/TrajEdge/TrajEdge-storage/target/TrajEdge-storage-1.0-SNAPSHOT.jar "$container":/tmp
  
  docker exec "$container" pkill -f TrajEdge-storage-1.0-SNAPSHOT.jar
  
  echo "Starting TrajEdge-storage-1.0-SNAPSHOT.jar in $container"
  # 在容器中执行 java -jar 命令
  docker exec "$container" java -jar /tmp/TrajEdge-storage-1.0-SNAPSHOT.jar 9999 &
done

echo "All operations completed."
