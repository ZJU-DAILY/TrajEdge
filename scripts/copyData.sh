#!/bin/bash

# 遍历 supervisor-i 和 supervisor-j
for i in {1..10}
do
    docker cp /home/hch/PROJECT/data supervisor-$i:/opt/
done