# TrajEdge: An Efficient and Lightweight Trajectory Data Analysis Framework in Edge Environment

## Introduction

This repository holds source code for the paper "TrajEdge: An Efficient and Lightweight Trajectory Data Analysis Framework in Edge Environment".

## Environment Preparation

- Java 11
- Docker
- CentOS 7.0

To set a edge simulated environment, you need to install Docker first, then configure the virtual network:

```
docker network create --driver bridge my-bridge-network
```

 Then you can use [tcconfig](https://tcconfig.readthedocs.io/en/latest/pages/usage/tcset/index.html#basic-usage) to set the network latency and bandwidth between docker container.

```
tcset eth0 --delay 100ms --rate 100Kbps --network 192.168.0.10
```

## Datasets Description

We use 2 publicly available real-world trajectory and road map data, which can be obtained from [Geolife](https://research.microsoft.com/en-us/projects), [T-Drive](http://www.geolink.pt/ecmlpkdd2015-challenge/dataset.html). And the synthetic dataset of Oldenburg can be generated in [LINK](https://research.microsoft.com/en-us/projects).

## Usage

1. Prepare your trajectory data like below:

   ```
   [Lat] [Lng] [TimeStamp]
   ```

   Each column is separated by a blank.

2. Modify your data loader class in directory `Spout`

3. Pack the project into fat jar `TrajEdge.jar` using Maven 

4. Run the topology `TrajectoryUploadTopology` to store trajectory data

   ```
   java -jar TrajEdge.jar --classpath org.example.TrajectoryUploadTopology
   ```

5. Run the query topology, including  `TrajectoryIdQueryTopology` , `TrajectorySpacialRangeQueryTopology` and `TrajectoryTimeRangeQueryTopology`.