#!/bin/bash

# 设置随机种子
RANDOM=$$

# 经纬度范围S
S_LAT_MIN=35.0
S_LAT_MAX=40.0
S_LON_MIN=-120.0
S_LON_MAX=-115.0

# h的值
h=3

# 生成30个随机经纬度值
declare -a LATS
declare -a LONS
for i in {1..30}; do
    LATS[$i]=$(bc -l <<< "scale=4; $RANDOM/32768*($S_LAT_MAX-$S_LAT_MIN)+$S_LAT_MIN")
    LONS[$i]=$(bc -l <<< "scale=4; $RANDOM/32768*($S_LON_MAX-$S_LON_MIN)+$S_LON_MIN")
done

# 函数：找到中心点
find_center() {
    local lat_min=$1
    local lat_max=$2
    local lon_min=$3
    local lon_max=$4
    echo "Center: $(bc -l <<< "scale=4; ($lat_min+$lat_max)/2"), $(bc -l <<< "scale=4; ($lon_min+$lon_max)/2")"
}

# 函数：计算距离
calc_distance() {
    local lat1=$1
    local lon1=$2
    local lat2=$3
    local lon2=$4
    local R=6371000 # 地球半径，单位：米
    local lat1_rad=$(echo "scale=10; $lat1 * 3.141592653589793 / 180" | bc -l)
    local lon1_rad=$(echo "scale=10; $lon1 * 3.141592653589793 / 180" | bc -l)
    local lat2_rad=$(echo "scale=10; $lat2 * 3.141592653589793 / 180" | bc -l)
    local lon2_rad=$(echo "scale=10; $lon2 * 3.141592653589793 / 180" | bc -l)
    local dLat=$(echo "scale=10; $lat2_rad - $lat1_rad" | bc -l)
    local dLon=$(echo "scale=10; $lon2_rad - $lon1_rad" | bc -l)
    local a=$(echo "scale=10; sin($dLat) * sin($dLat) + cos($lat1_rad) * cos($lat2_rad) * sin($dLon) * sin($dLon)" | bc -l)
    local c=$(echo "scale=10; 2 * atan2(sqrt($a), sqrt(1-$a))" | bc -l)
    echo "scale=2; $R * $c" | bc -l
}

# 函数：找到最近的点
find_nearest() {
    local center_lat=$1
    local center_lon=$2
    local min_distance=1e+99
    local nearest_index=0
    for i in "${!LATS[@]}"; do
        local distance=$(calc_distance $center_lat $center_lon ${LATS[$i]} ${LONS[$i]})
        if (( $(echo "$distance < $min_distance" | bc -l) )); then
            min_distance=$distance
            nearest_index=$i
        fi
    done
    echo "${LATS[$nearest_index]}, ${LONS[$nearest_index]}"
}

# 递归函数
divide_and_assign() {
    local lat_min=$1
    local lat_max=$2
    local lon_min=$3
    local lon_max=$4
    local depth=$5

    if [ $depth -le 0 ]; then
        return
    fi

    find_center $lat_min $lat_max $lon_min $lon_max
    nearest=$(find_nearest $(bc -l <<< "scale=4; ($lat_min+$lat_max)/2") $(bc -l <<< "scale=4; ($lon_min+$lon_max)/2"))
    echo "分配给最近的点：$nearest"

    local mid_lat=$(bc -l <<< "scale=4; ($lat_min+$lat_max)/2")
    local mid_lon=$(bc -l <<< "scale=4; ($lon_min+$lon_max)/2")

    # 左上
    divide_and_assign $lat_min $mid_lat $lon_min $mid_lon $((depth-1))
    # 右上
    divide_and_assign $mid_lat $lat_max $lon_min $mid_lon $((depth-1))
    # 左下
    divide_and_assign $lat_min $mid_lat $mid_lon $lon_max $((depth-1))
    # 右下
    divide_and_assign $mid_lat $lat_max $mid_lon $lon_max $((depth-1))
}

# 初始范围
divide_and_assign $S_LAT_MIN $S_LAT_MAX $S_LON_MIN $S_LON_MAX $h
