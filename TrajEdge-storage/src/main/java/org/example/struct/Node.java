package org.example.struct;
import java.util.List;

import org.example.trajstore.TrajStore;
import org.example.trajstore.TrajStoreConfig;
import org.example.trajstore.TrajStoreException;
import org.example.trajstore.TrajPoint;
import org.example.trajstore.FilterOptions;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.DaemonConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import java.io.File;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import org.example.grpc.TrajectoryServiceGrpc;
import org.example.grpc.TrajectoryRequest;
import org.example.grpc.TrajectoryResponse;
import org.example.grpc.TrajectoryPoint;


public class Node extends TrajectoryServiceGrpc.TrajectoryServiceImplBase{
    private static final Logger LOG = LoggerFactory.getLogger(Node.class);
    private static final String projectPath = new File(".").getAbsolutePath();
    
    private Integer port;
    private String id; // 节点ID
    private STHTIndex index; // STHT索引
    private TrajStore store;
    private ManagedChannel channel;
    private TrajectoryServiceGrpc.TrajectoryServiceBlockingStub blockingStub;

    public Node(String id) {
        this.id = id;
    }

    public Node(String id, Integer port) {
        this.id = id;
        this.port = port;
        this.index = new STHTIndex();
    
        Map<String, Object> conf = new HashMap<>();
        conf.put(DaemonConfig.STORM_METRIC_STORE_CLASS, "org.example.trajstore.rocksdb.RocksDbStore");
        conf.put(DaemonConfig.STORM_ROCKSDB_LOCATION, projectPath + "/output/data_" + id);
        conf.put(DaemonConfig.STORM_ROCKSDB_CREATE_IF_MISSING, true);
        conf.put(DaemonConfig.STORM_ROCKSDB_METADATA_STRING_CACHE_CAPACITY, 4000);
        conf.put(DaemonConfig.STORM_ROCKSDB_METRIC_RETENTION_HOURS, 240);
        try {
            store = TrajStoreConfig.configure(conf);
        } catch (TrajStoreException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void addTrajectoryData(TrajectoryRequest request, StreamObserver<TrajectoryResponse> responseObserver) {
        List<TrajPoint> trajectory = convertToTrajPoints(request.getPointsList());
        String result = addData(trajectory);

        TrajectoryResponse response = TrajectoryResponse.newBuilder()
                .setNextNodeId(result)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void readTrajectoryData(TrajectoryRequest request, StreamObserver<TrajectoryResponse> responseObserver) {
        List<TrajPoint> trajPoints = readData(request.getStartTime(), request.getEndTime(),
                request.getMinLat(), request.getMaxLat(), request.getMinLng(), request.getMaxLng());

        TrajectoryResponse.Builder responseBuilder = TrajectoryResponse.newBuilder();
        for (TrajPoint point : trajPoints) {
            responseBuilder.addPoints(convertToTrajectoryPoint(point));
        }

        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    private String addData(List<TrajPoint> trajectory) {
        String nextNode = index.insertTrajectory(trajectory);
        if (nextNode.isEmpty()) {
            storeData(trajectory);
        }
        return nextNode;
    }

    private void storeData(List<TrajPoint> trajectory) {
        for (TrajPoint point : trajectory) {
            try {
                synchronized (store) {
                    store.insert(point);
                }
                LOG.debug(point.toString());
            } catch (TrajStoreException e) {
                e.printStackTrace();
            }
        }
    }

    // 这种是递归式的读取数据
    public List<TrajPoint> readData(long startTime, long endTime, 
        double minLat, double maxLat, double minLng, double maxLng) {
        LOG.info("Query Condition: " + startTime + " " + endTime + " " + 
        minLat + " " + maxLat + " " + minLng + " " + maxLng);
        
        List<TrajPoint> trajPoints = new ArrayList<>();
        Set<Node> remoteNodes = new HashSet<>();
        List<Integer> localTrajIds = index.query(startTime, endTime, minLat, maxLat, minLng, maxLng, remoteNodes);

        // 1. 本地读取
        for (Integer id : localTrajIds) {
            try {
                List<TrajPoint> trajectoryPoints = doQuery(id, startTime, endTime);

                for (TrajPoint point : trajectoryPoints) {
                    if (point.getOriLat() >= minLat && point.getOriLat() <= maxLat &&
                        point.getOriLng() >= minLng && point.getOriLng() <= maxLng &&
                        point.getTimestamp() >= startTime && point.getTimestamp() <= endTime) {
                        trajPoints.add(point);
                    }
                }
            } catch (TrajStoreException e) {
                LOG.error("Error reading local trajectory data for ID: " + id, e);
            }
        }

        // 2. 远程读取
        for (Node node : remoteNodes) {
            try {
                if(node == null)continue;
                String remoteNodeAddress = getRemoteNodeAddress(node.id);
                List<TrajPoint> remotePoints = remoteReadData(remoteNodeAddress, startTime, endTime, minLat, maxLat, minLng, maxLng);
                trajPoints.addAll(remotePoints);
            } catch (Exception e) {
                LOG.error("Error reading remote data from node: " + node.id, e);
            }
        }

        return trajPoints;
    }

    private List<TrajPoint> doQuery(Integer id, long startTime, long endTime) throws TrajStoreException{
        FilterOptions filter = new FilterOptions();
        List<TrajPoint> list = new ArrayList<>();
        if (id != -1) {
            filter.setTrajectoryId(id);
        }
        if (startTime != -1) {
            filter.setStartTime(startTime);
        }
        if (endTime != -1) {
            filter.setEndTime(endTime);
        }
        store.scan(filter, list::add);
        if (list.isEmpty()) {
            LOG.info("There is no trajectory {}.", id);
        }
        else{
            for (TrajPoint p : list) {
                LOG.info(p.toString());
            }
        }
        return list;
    }

    private String getRemoteNodeAddress(String nodeId) {
        return nodeId + port;
    }

    // 辅助方法：远程读取数据
    private List<TrajPoint> remoteReadData(String remoteNodeAddress, long startTime, long endTime,
                                           double minLat, double maxLat, double minLng, double maxLng) {
        LOG.info("Reading data from remote node: " + remoteNodeAddress);
        
        // 创建 gRPC channel 和 stub
        channel = ManagedChannelBuilder.forTarget(remoteNodeAddress)
            .usePlaintext() // 为了简单起见，这里使用不安全的连接。在生产环境中应该使用 SSL/TLS
            .build();
        blockingStub = TrajectoryServiceGrpc.newBlockingStub(channel);

        try {
            // 创建请求
            TrajectoryRequest request = TrajectoryRequest.newBuilder()
                .setStartTime(startTime)
                .setEndTime(endTime)
                .setMinLat(minLat)
                .setMaxLat(maxLat)
                .setMinLng(minLng)
                .setMaxLng(maxLng)
                .build();

            // 发送 gRPC 请求并获取响应
            TrajectoryResponse response = blockingStub.readTrajectoryData(request);

            // 将 gRPC 响应转换为 TrajPoint 列表
            List<TrajPoint> trajPoints = new ArrayList<>();
            for (TrajectoryPoint point : response.getPointsList()) {
                TrajPoint trajPoint = new TrajPoint(
                    point.getTrajId(),
                    point.getTimestamp(),
                    point.getEdgeId(),
                    point.getDistance(),
                    point.getLat(),
                    point.getLng()
                );
                trajPoints.add(trajPoint);
            }

            return trajPoints;
        } catch (Exception e) {
            LOG.error("Error reading data from remote node: " + remoteNodeAddress, e);
            return new ArrayList<>();
        } finally {
            // 关闭 channel
            if (channel != null && !channel.isShutdown()) {
                channel.shutdown();
            }
        }
    }

    private List<TrajPoint> convertToTrajPoints(List<TrajectoryPoint> points) {
        List<TrajPoint> trajPoints = new ArrayList<>();
        for (TrajectoryPoint point : points) {
            trajPoints.add(new TrajPoint(
                    point.getTrajId(),
                    point.getTimestamp(),
                    point.getEdgeId(),
                    point.getDistance(),
                    point.getLat(),
                    point.getLng()
            ));
        }
        return trajPoints;
    }

    private TrajectoryPoint convertToTrajectoryPoint(TrajPoint point) {
        return TrajectoryPoint.newBuilder()
                .setTrajId(point.getTrajId())
                .setTimestamp(point.getTimestamp())
                .setEdgeId(point.getEdgeId())
                .setDistance(point.getDistance())
                .setLat(point.getOriLat())
                .setLng(point.getOriLng())
                .build();
    }
}