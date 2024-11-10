package org.example;

import org.example.grpc.TrajectoryRequest;
import org.example.grpc.TrajectoryResponse;
import org.example.grpc.TrajectoryPoint;
import io.grpc.stub.StreamObserver;
import org.junit.Test;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class NodeScaleTest {
    private static final String DATA_DIR = "/home/hch/PROJECT/data/geolife";
    private static final int BASE_PORT = 6700;
    private static final int[] NODE_SCALES = {6, 30, 150, 750};
    private static final int QUERY_TEST_COUNT = 100; // 执行100次查询来计算平均查询时间

    @Test
    public void testNodeScaling() throws Exception {
        for (int nodeCount : NODE_SCALES) {
            System.out.println("\n=== Testing with " + nodeCount + " nodes ===");
            
            // 启动指定数量的节点
            List<NodesService> nodes = startNodes(nodeCount);
            
            // 测试写入性能
            long writeTime = testWritePerformance(nodes);
            System.out.println("Write time for " + nodeCount + " nodes: " + writeTime + "ms");
            
            // 测试查询性能
            double avgQueryTime = testQueryPerformance(nodes);
            System.out.println("Average query time for " + nodeCount + " nodes: " + avgQueryTime + "ms");
            
            // 清理节点
            nodes.clear();
            System.gc(); // 建议进行垃圾回收
            Thread.sleep(1000); // 给系统一些时间清理资源

            break;
        }
    }

    private List<NodesService> startNodes(int count) {
        List<NodesService> nodes = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            nodes.add(new NodesService("localhost", BASE_PORT + i, false));
        }
        return nodes;
    }

    private long testWritePerformance(List<NodesService> nodes) throws Exception {
        List<TrajectoryPoint> allPoints = loadGeolifeData();
        
        // 按轨迹ID分组
        Map<Integer, List<TrajectoryPoint>> trajectories = allPoints.stream()
                .collect(Collectors.groupingBy(TrajectoryPoint::getTrajId));
        
        long startTime = System.currentTimeMillis();
        
        // 将轨迹平均分配给所有节点
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        List<List<TrajectoryPoint>> trajList = new ArrayList<>(trajectories.values());
        int trajPerNode = (int) Math.ceil((double) trajList.size() / nodes.size());
        
        for (int i = 0; i < nodes.size(); i++) {
            final int nodeIndex = i;
            int startIndex = i * trajPerNode;
            int endIndex = Math.min((i + 1) * trajPerNode, trajList.size());
            
            if (startIndex >= trajList.size()) {
                break;
            }
            
            List<List<TrajectoryPoint>> nodeTrajList = trajList.subList(startIndex, endIndex);
            
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                for (List<TrajectoryPoint> traj : nodeTrajList) {
                    writeTrajectoryToNode(nodes.get(nodeIndex), traj);
                }
            });
            futures.add(future);
        }
        
        // 等待所有写入操作完成
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        
        return System.currentTimeMillis() - startTime;
    }

    private double testQueryPerformance(List<NodesService> nodes) {
        long totalQueryTime = 0;
        Random random = new Random();
        
        for (int i = 0; i < QUERY_TEST_COUNT; i++) {
            // 随机选择一个节点
            NodesService node = nodes.get(random.nextInt(nodes.size()));
            
            // 创建查询请求
            TrajectoryRequest request = TrajectoryRequest.newBuilder()
                    .setQueryType(3)
                    .setTrajId(-1)
                    .setTopk(-1)
                    .setStartTime(1253769789L)
                    .setEndTime(1253769806L)
                    .setMinLat(39.97)
                    .setMaxLat(40.00)
                    .setMinLng(116.33)
                    .setMaxLng(116.34)
                    .build();

            long startTime = System.currentTimeMillis();
            
            // 执行查询
            CompletableFuture<TrajectoryResponse> future = new CompletableFuture<>();
            node.readTrajectoryData(request, new StreamObserver<TrajectoryResponse>() {
                @Override
                public void onNext(TrajectoryResponse value) {
                    future.complete(value);
                }

                @Override
                public void onError(Throwable t) {
                    future.completeExceptionally(t);
                }

                @Override
                public void onCompleted() {
                }
            });

            try {
                future.get(5, TimeUnit.SECONDS); // 设置超时时间为5秒
                totalQueryTime += System.currentTimeMillis() - startTime;
            } catch (Exception e) {
                System.err.println("Query failed: " + e.getMessage());
            }
        }
        
        return (double) totalQueryTime / QUERY_TEST_COUNT;
    }

    private void writeTrajectoryToNode(NodesService node, List<TrajectoryPoint> trajectory) {
        TrajectoryRequest request = TrajectoryRequest.newBuilder()
                .addAllPoints(trajectory)
                .build();

        CompletableFuture<TrajectoryResponse> future = new CompletableFuture<>();
        node.addTrajectoryData(request, new StreamObserver<TrajectoryResponse>() {
            @Override
            public void onNext(TrajectoryResponse value) {
                future.complete(value);
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {
            }
        });

        try {
            future.get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            System.err.println("Write failed for trajectory " + trajectory.get(0).getTrajId() + ": " + e.getMessage());
        }
    }

    private List<TrajectoryPoint> loadGeolifeData() throws IOException {
        List<TrajectoryPoint> points = new ArrayList<>();
        File path = new File(DATA_DIR);
        String[] fileList = path.list();
        
        if (fileList == null) {
            throw new IOException("Cannot list files in directory: " + DATA_DIR);
        }

        for (String filename : fileList) {
            if (!filename.endsWith(".txt")) continue;
            
            String filePath = DATA_DIR + "/" + filename;
            try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
                int trajId = points.size() / 1000 + 1; // 每1000个点作为一条轨迹
                int edgeId = 0;
                String line;
                
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.strip().split("\\s+");
                    if (parts.length >= 3) {
                        double lat = Double.parseDouble(parts[0]);
                        double lng = Double.parseDouble(parts[1]);
                        long timestamp = Long.parseLong(parts[2]);
                        
                        TrajectoryPoint point = TrajectoryPoint.newBuilder()
                                .setTrajId(trajId)
                                .setTimestamp(timestamp)
                                .setEdgeId(edgeId++)
                                .setDistance(0.0) // 如果没有距离信息，设为0
                                .setLat(lat)
                                .setLng(lng)
                                .build();
                        points.add(point);
                    }
                }
            } catch (IOException e) {
                System.err.println("Error reading file: " + filePath);
                e.printStackTrace();
            }
        }
        
        return points;
    }
} 