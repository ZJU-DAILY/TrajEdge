package org.example;

import org.example.grpc.TrajectoryRequest;
import org.example.grpc.TrajectoryResponse;
import org.example.grpc.TrajectoryServiceGrpc;
import org.example.grpc.TrajectoryPoint;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.Test;

import com.google.gson.Gson;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class NodeScaleTest {
    // 启动节点进程
    List<Process> nodeProcesses = new ArrayList<>();
    // 添加新的字段来存储 gRPC 客户端
    private List<TrajectoryServiceGrpc.TrajectoryServiceBlockingStub> stubs = new ArrayList<>();
    private static final String DATA_DIR = "/home/hch/PROJECT/data/geolife/trajectory";
    private static final int BASE_PORT = 10000;
    private static final int QUERY_TEST_COUNT = 1;
    private static final double LAT_MIN = -90;
    private static final double LAT_MAX = 90;
    private static final double LNG_MIN = 180;
    private static final double LNG_MAX = -180;
    private static final int TREE_HEIGHT = 4;

    @Test
    public void testNodeScaling6() throws Exception {
        runScaleTest(6);
    }

    @Test
    public void testNodeScaling30() throws Exception {
        runScaleTest(30);
    }

    @Test
    public void testNodeScaling150() throws Exception {
        runScaleTest(150);
    }

    @Test
    public void testNodeScaling750() throws Exception {
        runScaleTest(750);
    }

    private void runScaleTest(int nodeCount) throws Exception {
        System.out.println("\n=== Testing with " + nodeCount + " nodes ===");
        try {
            // 启动指定数量的节点
            List<Process> nodeProcesses = startNodes(nodeCount);
            
            // 测试写入性能
            long writeTime = testWritePerformance(nodeCount);
            System.out.println("Write time for " + nodeCount + " nodes: " + writeTime + "ms");
            
            // 测试查询性能
            double avgQueryTime = testQueryPerformance(nodeCount);
            System.out.println("Average query time for " + nodeCount + " nodes: " + avgQueryTime + "ms");
            
            // 清理节点进程
            for (Process process : nodeProcesses) {
                process.destroy();
            }
            Thread.sleep(1000); // 给系统一些时间清理资源
        } finally {
            // 清理 gRPC 客户端
            for (TrajectoryServiceGrpc.TrajectoryServiceBlockingStub stub : stubs) {
                ManagedChannel channel = (ManagedChannel) stub.getChannel();
                if (channel != null && !channel.isShutdown()) {
                    channel.shutdown();
                }
            }
            // 确保真正杀死进程
            for (Process process : nodeProcesses) {
                process.destroyForcibly(); // 使用 destroyForcibly 确保进程被杀死
            }
        }
    }

    private List<Process> startNodes(int count) throws Exception {
        // 生成随机点
        List<Point> points = generateRandomPoints(count);
        
        // 构建四叉树并生成节点关系
        QuadTreeNode root = buildQuadTree("#", LAT_MIN, LAT_MAX, LNG_MIN, LNG_MAX, TREE_HEIGHT, points);
        
        // 为每个节点创建节点关系映射
        Map<Integer, Map<String, Map<String, String>>> nodeInfosMap = generateNodeInfos(root, points, count);
        
        nodeProcesses.clear();
        stubs.clear();
        for (int i = 0; i < count; i++) {
            int port = BASE_PORT + i;
            // 将节点配置写入临时文件或环境变量
            Map<String, Map<String, String>> nodeInfo = nodeInfosMap.get(i + 1);
            
            // 构建进程启动命令
            ProcessBuilder processBuilder = new ProcessBuilder(
                "java",
                "-cp",
                System.getProperty("java.class.path"),
                "org.example.NodeLauncher",
                String.valueOf(port)
            );
            // 添加错误流重定向
            processBuilder.redirectErrorStream(true);
            
            // 设置环境变量
            Map<String, String> env = processBuilder.environment();
            env.put("CONTAINER_ID", "supervisor-" + (i + 1));
            env.put("NODE_INFO", convertMapToString(nodeInfo)); // 将Map转换为字符串形式
            
            // 启动进程
            Process process = processBuilder.start();
            nodeProcesses.add(process);
            
            // 读取错误信息
            // try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            //     String line;
            //     while ((line = reader.readLine()) != null) {
            //         System.err.println(line); // 打印错误信息
            //     }
            // }
            // 创建 gRPC 客户端
            ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", port)
                .usePlaintext()
                .build();
            TrajectoryServiceGrpc.TrajectoryServiceBlockingStub stub = 
                TrajectoryServiceGrpc.newBlockingStub(channel);
            stubs.add(stub);
            
            // 等待一小段时间确保进程启动
            Thread.sleep(1000);
        }
        
        return nodeProcesses;
    }

    private static class Point {
        double lat, lng;
        Point(double lat, double lng) {
            this.lat = lat;
            this.lng = lng;
        }
    }

    private static class QuadTreeNode {
        String prefix;
        double latMin, latMax, lngMin, lngMax;
        Point nearestPoint;
        List<QuadTreeNode> children = new ArrayList<>();
        QuadTreeNode parent;
        QuadTreeNode leftNeighbor;
        QuadTreeNode rightNeighbor;

        QuadTreeNode(String prefix, double latMin, double latMax, double lngMin, double lngMax) {
            this.prefix = prefix;
            this.latMin = latMin;
            this.latMax = latMax;
            this.lngMin = lngMin;
            this.lngMax = lngMax;
        }
    }

    private List<Point> generateRandomPoints(int count) {
        List<Point> points = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < count; i++) {
            double lat = LAT_MIN + random.nextDouble() * (LAT_MAX - LAT_MIN);
            double lng = LNG_MIN + random.nextDouble() * (LNG_MAX - LNG_MIN);
            points.add(new Point(lat, lng));
        }
        return points;
    }

    private QuadTreeNode buildQuadTree(String prefix, double latMin, double latMax, 
                                     double lngMin, double lngMax, int depth, List<Point> points) {
        QuadTreeNode node = new QuadTreeNode(prefix, latMin, latMax, lngMin, lngMax);
        
        if (depth == 0) {
            return node;
        }

        Point center = new Point((latMin + latMax) / 2, (lngMin + lngMax) / 2);
        node.nearestPoint = findNearestPoint(center, points);

        if (depth > 0) {
            double midLat = (latMin + latMax) / 2;
            double midLng = (lngMin + lngMax) / 2;

            // 创建四个子节点
            QuadTreeNode lu = buildQuadTree(prefix + "0", latMin, midLat, lngMin, midLng, depth - 1, points);
            QuadTreeNode ru = buildQuadTree(prefix + "1", midLat, latMax, lngMin, midLng, depth - 1, points);
            QuadTreeNode ld = buildQuadTree(prefix + "2", latMin, midLat, midLng, lngMax, depth - 1, points);
            QuadTreeNode rd = buildQuadTree(prefix + "3", midLat, latMax, midLng, lngMax, depth - 1, points);

            // 设置父子关系
            lu.parent = ru.parent = ld.parent = rd.parent = node;
            node.children.addAll(Arrays.asList(lu, ru, ld, rd));

            // 设置邻居关系
            lu.rightNeighbor = ru;
            ru.leftNeighbor = lu;
            ld.rightNeighbor = rd;
            rd.leftNeighbor = ld;
            lu.rightNeighbor = ld;
            ld.leftNeighbor = lu;
            ru.rightNeighbor = rd;
            rd.leftNeighbor = ru;
        }

        return node;
    }

    private Map<Integer, Map<String, Map<String, String>>> generateNodeInfos(QuadTreeNode root, List<Point> points, int nodeCount) {
        Map<Integer, Map<String, Map<String, String>>> nodeInfosMap = new HashMap<>();
        
        // 初始化每个节点的 Map
        for (int i = 1; i <= nodeCount; i++) {
            nodeInfosMap.put(i, new TreeMap<>());
        }
        
        // 递归处理所有节点
        processNodeInfos(root, points, nodeInfosMap);
        
        return nodeInfosMap;
    }

    private void processNodeInfos(QuadTreeNode node, List<Point> points, 
                                  Map<Integer, Map<String, Map<String, String>>> nodeInfosMap) {
        if (node.nearestPoint != null) {
            int currentNodeId = getPointIndex(points, node.nearestPoint) + 1;
            Map<String, Map<String, String>> currentNodeInfo = nodeInfosMap.get(currentNodeId);
            
            // 初始化当前前缀的关系映射
            currentNodeInfo.put(node.prefix, new HashMap<>());
            
            // 添加子节点信息
            for (QuadTreeNode child : node.children) {
                if (child.nearestPoint != null) {
                    int childId = getPointIndex(points, child.nearestPoint) + 1;
                    currentNodeInfo.get(node.prefix).put(child.prefix, String.valueOf(BASE_PORT + childId - 1));
                }
            }
            
            // 添加父节点信息
            if (node.parent != null && node.parent.nearestPoint != null) {
                int parentId = getPointIndex(points, node.parent.nearestPoint) + 1;
                currentNodeInfo.get(node.prefix).put(node.parent.prefix, String.valueOf(BASE_PORT + parentId - 1));
            }
            
            // 添加邻居节点信息
            if (node.leftNeighbor != null && node.leftNeighbor.nearestPoint != null) {
                int neighborId = getPointIndex(points, node.leftNeighbor.nearestPoint) + 1;
                currentNodeInfo.get(node.prefix).put(node.leftNeighbor.prefix, String.valueOf(BASE_PORT + neighborId - 1));
            }
            if (node.rightNeighbor != null && node.rightNeighbor.nearestPoint != null) {
                int neighborId = getPointIndex(points, node.rightNeighbor.nearestPoint) + 1;
                currentNodeInfo.get(node.prefix).put(node.rightNeighbor.prefix, String.valueOf(BASE_PORT + neighborId - 1));
            }
        }
        
        // 递归处理子节点
        for (QuadTreeNode child : node.children) {
            processNodeInfos(child, points, nodeInfosMap);
        }
    }

    private Point findNearestPoint(Point center, List<Point> points) {
        Point nearest = null;
        double minDistance = Double.MAX_VALUE;
        for (Point point : points) {
            double distance = calculateDistance(center.lat, center.lng, point.lat, point.lng);
            if (distance < minDistance) {
                minDistance = distance;
                nearest = point;
            }
        }
        return nearest;
    }

    private int getPointIndex(List<Point> points, Point point) {
        for (int i = 0; i < points.size(); i++) {
            if (points.get(i) == point) {
                return i;
            }
        }
        return -1;
    }

    private double calculateDistance(double lat1, double lng1, double lat2, double lng2) {
        final int R = 6371; // Earth's radius in kilometers
        double latDistance = Math.toRadians(lat2 - lat1);
        double lngDistance = Math.toRadians(lng2 - lng1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(lngDistance / 2) * Math.sin(lngDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c;
    }

    private long testWritePerformance(int nodeCount) throws Exception {
        List<TrajectoryPoint> allPoints = loadGeolifeData();
        
        // 按轨迹ID分组
        Map<Integer, List<TrajectoryPoint>> trajectories = allPoints.stream()
                .collect(Collectors.groupingBy(TrajectoryPoint::getTrajId));
        
        long startTime = System.currentTimeMillis();
        
        // 将轨迹平均分配给所有节点
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        List<List<TrajectoryPoint>> trajList = new ArrayList<>(trajectories.values());
        int trajPerNode = (int) Math.ceil((double) trajList.size() / nodeCount);
        
        for (int i = 0; i < nodeCount; i++) {
            final int nodeIndex = i;
            int startIndex = i * trajPerNode;
            int endIndex = Math.min((i + 1) * trajPerNode, trajList.size());
            
            if (startIndex >= trajList.size()) {
                break;
            }
            
            List<List<TrajectoryPoint>> nodeTrajList = trajList.subList(startIndex, endIndex);
            
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                for (List<TrajectoryPoint> traj : nodeTrajList) {
                    writeTrajectoryToNode(nodeIndex, traj);
                }
            });
            futures.add(future);
        }
        
        // 等待所有写入操作完成
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        
        return System.currentTimeMillis() - startTime;
    }

    private double testQueryPerformance(int nodeCount) {
        long totalQueryTime = 0;
        Random random = new Random();
        
        for (int i = 0; i < QUERY_TEST_COUNT; i++) {
            int nodeIndex = random.nextInt(nodeCount);
            
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
            
            try {
                TrajectoryResponse response = stubs.get(nodeIndex).readTrajectoryData(request);
                totalQueryTime += System.currentTimeMillis() - startTime;
            } catch (Exception e) {
                e.printStackTrace(); 
                System.err.println("Query failed: " + e.getMessage());
            }
        }
        
        return (double) totalQueryTime / QUERY_TEST_COUNT;
    }

    private void writeTrajectoryToNode(int nodeIndex, List<TrajectoryPoint> trajectory) {
        TrajectoryRequest request = TrajectoryRequest.newBuilder()
                .addAllPoints(trajectory)
                .build();

        try {
            Integer index = nodeIndex;
            while(true){
                TrajectoryResponse response = stubs.get(index).addTrajectoryData(request);
                String port = response.getNextNodeId();
                if(port.isEmpty())break;
                index = Integer.parseInt(port) - BASE_PORT;
            }
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
            // if (!filename.endsWith(".txt")) continue;
            
            String filePath = DATA_DIR + "/" + filename;
            try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
                int trajId = Integer.parseInt(filename); // 每1000个点作为一条轨迹
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
                if(points.size() >= 5000)break;
            } catch (IOException e) {
                System.err.println("Error reading file: " + filePath);
                e.printStackTrace();
            }
        }
        
        return points;
    }

    // 辅助方法：将Map转换为字符串
    private String convertMapToString(Map<String, Map<String, String>> nodeInfo) {
        // 实现Map到字符串的转换逻辑
        // 可以使用JSON或其他序列化方式
        return new Gson().toJson(nodeInfo);
    }
} 