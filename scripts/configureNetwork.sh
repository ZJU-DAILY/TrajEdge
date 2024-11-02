#!/bin/bash

# 遍历 supervisor-i 和 supervisor-j
for i in {1..10}
do
    for j in {1..10}
    do
        # 检查 i 和 j 是否相同，如果相同则跳过
        if [ "$i" -ne "$j" ]; then
            # 设置 supervisor-i 到 supervisor-j 的网络条件
            echo "Setting network conditions from supervisor-$i to supervisor-$j"
            tcset supervisor-$i --src-container supervisor-$i --dst-container supervisor-$j --docker --delay 20ms --rate 2Mbps
            
            # 设置 supervisor-j 到 supervisor-i 的网络条件
            # echo "Setting network conditions from supervisor-$j to supervisor-$i"
            # tcset supervisor-$j --src-container supervisor-$j --dst-container supervisor-$i --docker --delay 20ms --rate 2Mbps
        fi
    done
done