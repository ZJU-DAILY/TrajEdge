package org.example.struct;

import org.example.trajstore.TrajPoint;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import java.util.*;
import org.example.exp.dht.GeoTrie;
import org.example.exp.dht.QueryResult;
import org.example.exp.dht.SimpleDHT;
import org.example.exp.dht.DHTNode;
import org.example.exp.dht.DHTRoutingResult;
import org.example.exp.dht.DHTUtils;

public class NodeTest {
    private List<Node> nodes;
    private Random random;
    private static final int NUM_NODES = 30;
    private static final int NUM_TEST_POINTS = 290;
    private STHTIndex indexUtils;
    private static final double LAT_MIN = -90;
    private static final double LAT_MAX = 90;
    private static final double LNG_MIN = -180;
    private static final double LNG_MAX = 180;
    private static final int TREE_HEIGHT = 4;
    private GeoTrie geoTrie;
    private SimpleDHT simpleDHT;
    private Map<String, Point> nodeLocations; // 存储所有节点的位置信息
    
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
    
    @Before
    public void setUp() {
        nodes = new ArrayList<>();
        random = new Random(42); // 固定种子以获得可重复的结果
        indexUtils = new STHTIndex();
        
        // 生成随机点
        List<Point> points = generateRandomPoints(NUM_NODES);
        
        // 构建四叉树并生成节点关系
        QuadTreeNode root = buildQuadTree("#", LAT_MIN, LAT_MAX, LNG_MIN, LNG_MAX, TREE_HEIGHT, points);
        
        // 为每个节点创建节点关系映射
        Map<Integer, Map<String, Map<String, String>>> nodeInfosMap = generateNodeInfos(root, points, NUM_NODES);
        
        // 创建Node实例
        for (int i = 1; i <= NUM_NODES; i++) {
            Map<String, Map<String, String>> nodeInfo = nodeInfosMap.get(i);
            for (Map.Entry<String, Map<String, String>> entry : nodeInfo.entrySet()) {
                String prefix = entry.getKey();
                Map<String, String> routingEntry = entry.getValue();
                nodes.add(new Node(prefix, routingEntry, "docker_" + (i-1)));
            }
        }
        
        // 初始化节点位置信息
        nodeLocations = new HashMap<>();
        for (int i = 0; i < NUM_NODES; i++) {
            nodeLocations.put("docker_" + i, points.get(i));
            nodeLocations.put(DHTUtils.sha256("node_" + i), points.get(i));
        }
        
        // 初始化GeoTrie和SimpleDHT
        geoTrie = new GeoTrie(NUM_NODES, 32);
        simpleDHT = new SimpleDHT(NUM_NODES);
    }

    private List<Point> generateRandomPoints(int count) {
        List<Point> points = new ArrayList<>();
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

    private Map<Integer, Map<String, Map<String, String>>> generateNodeInfos(QuadTreeNode root, 
            List<Point> points, int nodeCount) {
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
                    currentNodeInfo.get(node.prefix).put(child.prefix, "docker_" + (childId - 1));
                }
            }
            
            // 添加父节点信息
            if (node.parent != null && node.parent.nearestPoint != null) {
                int parentId = getPointIndex(points, node.parent.nearestPoint) + 1;
                currentNodeInfo.get(node.prefix).put(node.parent.prefix, "docker_" + (parentId - 1));
            }
            
            // 添加邻居节点信息
            if (node.leftNeighbor != null && node.leftNeighbor.nearestPoint != null) {
                int neighborId = getPointIndex(points, node.leftNeighbor.nearestPoint) + 1;
                currentNodeInfo.get(node.prefix).put(node.leftNeighbor.prefix, "docker_" + (neighborId - 1));
            }
            if (node.rightNeighbor != null && node.rightNeighbor.nearestPoint != null) {
                int neighborId = getPointIndex(points, node.rightNeighbor.nearestPoint) + 1;
                currentNodeInfo.get(node.prefix).put(node.rightNeighbor.prefix, "docker_" + (neighborId - 1));
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

    @Test
    public void testInsertAndQuery() {
        // 统计数据
        Map<Integer, Integer> routingLengths = new HashMap<>();
        Map<String, Integer> nodeHits = new HashMap<>();
        
        // 生成测试数据并插入
        List<List<TrajPoint>> trajectories = generateTestTrajectories(LAT_MIN, LAT_MAX, LNG_MIN, LNG_MAX);
        System.out.println("Testing insertions...");
        
        for (List<TrajPoint> traj : trajectories) {
            // 选择随机起始节点
            Node startNode = nodes.get(random.nextInt(nodes.size()));
            
            // 使用STHTIndex生成key
            String key = indexUtils.encodeUniversalKey(traj);
            
            // 记录路由路径
            List<String> routingPath = new ArrayList<>();
            routingPath.add(startNode.getPrefix());
            
            Node currentNode = startNode;
            while (true) {
                String[] result = currentNode.insert(key, traj);
                if (result[0].isEmpty()) {
                    // 到达目标节点
                    nodeHits.merge(currentNode.getPrefix(), 1, Integer::sum);
                    break;
                }
                
                // 继续路由
                String nextPrefix = result[0];
                String nextDockerName = result[1];
                routingPath.add(nextPrefix);
                
                // 找到下一个节点
                currentNode = findNodeByPrefix(nextPrefix);
                if (currentNode == null) {
                    // 模拟跨docker的情况，在测试中直接结束
                    break;
                }
                nodeHits.merge(currentNode.getPrefix(), 1, Integer::sum);
            }
            
            // 记录路由长度
            routingLengths.merge(routingPath.size(), 1, Integer::sum);
            System.out.printf("Insert: Routing length = %d, Path = %s%n", 
                routingPath.size(), String.join(" -> ", routingPath));
        }
        
        // 打印统计信息
        printStatistics(routingLengths, nodeHits);
        
        // 测试范围查询
        System.out.println("\nTesting range query...");
        testRangeQuery();
    }
    
    private void testRangeQuery() {
        // 定义查询范围
        double minLat = -10, maxLat = 10;
        double minLng = -20, maxLng = 20;
        long startTime = 1609459200000L; // 2021-01-01
        long endTime = 1612137600000L;   // 2021-02-01
        
        Map<String, Integer> queryHits = new HashMap<>();
        List<TrajPoint> results = new ArrayList<>();
        
        // 生成key ranges
        List<KeyRange> keyRanges = indexUtils.gKeyRanges(startTime, endTime,
            minLat, maxLat, minLng, maxLng);
            
        // 从随机节点开始查询
        Node startNode = nodes.get(random.nextInt(nodes.size()));
        Set<Node> visitedNodes = new HashSet<>();
        
        // 对每个key range执行查询
        for (KeyRange range : keyRanges) {
            Iterator<Key> iter = range.iterator();
            while (iter.hasNext()) {
                String prefix = iter.next().getKey();
                queryNodeByPrefix(startNode, prefix, -1, startTime, endTime,
                    minLat, maxLat, minLng, maxLng, results, queryHits, visitedNodes);
            }
        }
        
        // 打印查询结果
        System.out.printf("Range Query: Visited %d nodes, Found %d points%n", 
            visitedNodes.size(), results.size());
        
        // 计算查询的命中率
        int totalHits = queryHits.values().stream().mapToInt(Integer::intValue).sum();
        queryHits.forEach((prefix, hits) -> {
            double hitRatio = (double) hits / totalHits;
            System.out.printf("Node %s hit ratio: %.4f%n", prefix, hitRatio);
        });
    }
    
    // private void queryNodeByPrefix(Node startNode, String targetPrefix, int trajId, 
    //                              long startTime, long endTime,
    //                              double minLat, double maxLat, double minLng, double maxLng,
    //                              List<TrajPoint> results, Map<String, Integer> queryHits,
    //                              Set<Node> visitedNodes) {
    //     Node currentNode = startNode;
    //     while (true) {
    //         if (visitedNodes.contains(currentNode)) {
    //             break;
    //         }
    //         visitedNodes.add(currentNode);
    //         queryHits.merge(currentNode.getPrefix(), 1, Integer::sum);
            
    //         String[] findResult = currentNode.findKey(targetPrefix);
    //         if (findResult[0].isEmpty()) {
    //             // 在前节点查询
    //             List<TrajPoint> nodeResults = currentNode.doRead(trajId, startTime, endTime,
    //                 minLat, maxLat, minLng, maxLng);
    //             results.addAll(nodeResults);
    //             break;
    //         } else {
    //             // 继续路由
    //             String nextPrefix = findResult[0];
    //             String nextDockerName = findResult[1];
    //             currentNode = findNodeByPrefix(nextPrefix);
    //             if (currentNode == null) {
    //                 // 模拟跨docker的情况，在测试中直接结束
    //                 break;
    //             }
    //         }
    //     }
    // }
    
    private List<List<TrajPoint>> generateTestTrajectories(double minLat, double maxLat, double minLng, double maxLng) {
        List<List<TrajPoint>> trajectories = new ArrayList<>();
        for (int i = 0; i < NUM_TEST_POINTS; i++) {
            List<TrajPoint> trajectory = new ArrayList<>();
            double lat = minLat + random.nextDouble() * (maxLat - minLat);
            double lng = minLng + random.nextDouble() * (maxLng - minLng);
            long timestamp = 1176341492L + random.nextInt((int)(1343349080L - 1176341492L)); // 使用相同的时间范围
            
            trajectory.add(new TrajPoint(i, timestamp, 0L, 0.0, lat, lng));
            trajectories.add(trajectory);
        }
        return trajectories;
    }
    
    private Node findNodeByPrefix(String prefix) {
        return nodes.stream()
            .filter(n -> n.getPrefix().equals(prefix))
            .findFirst()
            .orElse(null);
    }
    
    private void printStatistics(Map<Integer, Integer> routingLengths, 
                               Map<String, Integer> nodeHits) {
        // 计算平均路由长度
        double avgRoutingLength = routingLengths.entrySet().stream()
            .mapToDouble(e -> e.getKey() * e.getValue())
            .sum() / routingLengths.values().stream().mapToInt(Integer::intValue).sum();
        
        System.out.printf("Average routing length: %.2f%n", avgRoutingLength);
        
        // 算每个节点的命中率
        int totalHits = nodeHits.values().stream().mapToInt(Integer::intValue).sum();
        nodeHits.forEach((prefix, hits) -> {
            double hitRatio = (double) hits / totalHits;
            System.out.printf("Node %s hit ratio: %.4f%n", prefix, hitRatio);
        });
    }

    @Test
    public void compareRoutingPerformance() {
        // 测试不同数据集
        String[] datasets = {"tdrive", "geolife"};
        int[] rangeFactors = {8, 4, 2, 1};  // v值，表示取全局范围的1/v
        
        for (String dataset : datasets) {
            System.out.println("\n=== Testing " + dataset + " dataset ===");
            
            // 设置数据集的全局范围
            double minLat, maxLat, minLng, maxLng;
            if ("tdrive".equals(dataset)) {
                minLat = 0.0;
                maxLat = 65.20465;
                minLng = 0.0;
                maxLng = 174.06752;
            } else { // geolife
                minLat = 1.044024;
                maxLat = 63.0141583;
                minLng = -179.9695933;
                maxLng = 179.9969416;
            }
            
            // 使用相同范围生成测试数据
            List<List<TrajPoint>> trajectories = generateTestTrajectories(minLat, maxLat, minLng, maxLng);
            
            // 测试不同范围大小
            for (int v : rangeFactors) {
                System.out.printf("\n--- Testing range size 1/%d of global range ---%n", v);
                
                // 测试三种方法
                System.out.println("\n=== Original Node Implementation ===");
                testRangeQueryPerformance(trajectories, minLat, maxLat, minLng, maxLng, v);
                
                System.out.println("\n=== GeoTrie Implementation ===");
                testGeoTrieRangePerformance(trajectories, minLat, maxLat, minLng, maxLng, v);
                
                System.out.println("\n=== SimpleDHT Implementation ===");
                testSimpleDHTRangePerformance(trajectories, minLat, maxLat, minLng, maxLng, v);
            }
        }
    }
    
    private void testRangeQueryPerformance(List<List<TrajPoint>> trajectories,
                                         double globalMinLat, double globalMaxLat,
                                         double globalMinLng, double globalMaxLng,
                                         int rangeFactor) {
        double totalRoutingLength = 0;
        double totalRoutingTime = 0;
        int numQueries = 10; // 执行10次范围查询测试
        
        // 计算查询范围大小
        double latRange = (globalMaxLat - globalMinLat) / rangeFactor;
        double lngRange = (globalMaxLng - globalMinLng) / rangeFactor;
        
        for (int i = 0; i < numQueries; i++) {
            // 随机生成查询中心点
            double centerLat = globalMinLat + random.nextDouble() * (globalMaxLat - globalMinLat);
            double centerLng = globalMinLng + random.nextDouble() * (globalMaxLng - globalMinLng);
            
            // 计算查询范围
            double lat1 = Math.max(centerLat - latRange/2, globalMinLat);
            double lat2 = Math.min(centerLat + latRange/2, globalMaxLat);
            double lng1 = Math.max(centerLng - lngRange/2, globalMinLng);
            double lng2 = Math.min(centerLng + lngRange/2, globalMaxLng);
            
            long t1 = 1176341492L;  // 从Topology文件中获取的时间范围
            long t2 = 1343349080L;
            
            // 生成key ranges
            List<KeyRange> keyRanges = indexUtils.gKeyRanges(t1, t2, lat1, lat2, lng1, lng2);
            
            // 第一次查询从随机节点开始
            Node currentNode = nodes.get(random.nextInt(nodes.size()));
            Set<Node> visitedNodes = new HashSet<>();
            List<TrajPoint> results = new ArrayList<>();
            Set<Node> lengthCnt = new HashSet<>();
            
            // 执行查询
            for (KeyRange range : keyRanges) {
                Iterator<Key> iter = range.iterator();
                while (iter.hasNext()) {
                    String prefix = iter.next().getKey();
                    Point lastPoint = nodeLocations.get(currentNode.getDockerName());
                    
                    Node targetNode = queryNodeByPrefix(currentNode, prefix, -1, t1, t2,
                        lat1, lat2, lng1, lng2, results, new HashMap<>(), visitedNodes);
                    
                    if (targetNode != null && targetNode != currentNode) {
                        Point targetPoint = nodeLocations.get(targetNode.getDockerName());
                        totalRoutingTime += calculateDistance(
                            lastPoint.lat, lastPoint.lng,
                            targetPoint.lat, targetPoint.lng
                        );
                    }
                    lengthCnt.addAll(visitedNodes);
                    visitedNodes.clear();
                    if (targetNode != null) {
                        currentNode = targetNode;
                    }
                }
            }
            
            totalRoutingLength += lengthCnt.size();
        }
        
        // 输出平均结果
        System.out.printf("Average routing length: %.2f%n", totalRoutingLength / numQueries);
        System.out.printf("Average routing time (distance): %.2f%n", totalRoutingTime / numQueries);
    }
    
    private Node queryNodeByPrefix(Node startNode, String targetPrefix, int trajId, 
                                 long startTime, long endTime,
                                 double minLat, double maxLat, double minLng, double maxLng,
                                 List<TrajPoint> results, Map<String, Integer> queryHits,
                                 Set<Node> visitedNodes) {
        Node currentNode = startNode;
        Node targetNode = null;
        
        while (true) {
            if (visitedNodes.contains(currentNode)) {
                break;
            }
            visitedNodes.add(currentNode);
            queryHits.merge(currentNode.getPrefix(), 1, Integer::sum);
            
            String[] findResult = currentNode.findKey(targetPrefix);
            if (findResult[0].isEmpty()) {
                // 在当前节点查询
                List<TrajPoint> nodeResults = currentNode.doRead(trajId, startTime, endTime,
                    minLat, maxLat, minLng, maxLng);
                results.addAll(nodeResults);
                targetNode = currentNode;
                break;
            } else {
                // 继续路由
                String nextPrefix = findResult[0];
                String nextDockerName = findResult[1];
                currentNode = findNodeByPrefix(nextPrefix);
                if (currentNode == null) {
                    // 模拟跨docker的情况，在测试中直接结束
                    break;
                }
            }
        }
        
        return targetNode;
    }
    
    private void testGeoTrieRangePerformance(List<List<TrajPoint>> trajectories,
                                            double globalMinLat, double globalMaxLat,
                                            double globalMinLng, double globalMaxLng,
                                            int rangeFactor) {
        if(rangeFactor == 1){
            System.out.println("rangeFactor is 1, skip this test");
        } 
        // 首先插入所有轨迹点以构建树结构
        for (List<TrajPoint> traj : trajectories) {
            TrajPoint point = traj.get(0);
            geoTrie.insert(point.getOriLat(), point.getOriLng(), point.getTimestamp(), point);
        }
        
        double totalRoutingLength = 0;
        double totalRoutingTime = 0;
        int numQueries = 10;
        
        // 计算查询范围大小
        double latRange = (globalMaxLat - globalMinLat) / rangeFactor;
        double lngRange = (globalMaxLng - globalMinLng) / rangeFactor;
        
        for (int i = 0; i < numQueries; i++) {
            // 随机生成查询中心点
            double centerLat = globalMinLat + random.nextDouble() * (globalMaxLat - globalMinLat);
            double centerLng = globalMinLng + random.nextDouble() * (globalMaxLng - globalMinLng);
            
            // 计算查询范围
            double lat1 = Math.max(centerLat - latRange/2, globalMinLat);
            double lat2 = Math.min(centerLat + latRange/2, globalMaxLat);
            double lng1 = Math.max(centerLng - lngRange/2, globalMinLng);
            double lng2 = Math.min(centerLng + lngRange/2, globalMaxLng);
            
            long t1 = 1176341492L;
            long t2 = 1343349080L;
            
            QueryResult<Object> result = geoTrie.queryRange(lat1, lat2, lng1, lng2, t1, t2);
            
            totalRoutingLength += result.getRoutingLength();
            
            // 计算路由路径的总距离
            List<DHTNode> path = result.getPath();
            for (int j = 1; j < path.size(); j++) {
                Point p1 = nodeLocations.get(path.get(j-1).getNodeId());
                Point p2 = nodeLocations.get(path.get(j).getNodeId());
                totalRoutingTime += calculateDistance(p1.lat, p1.lng, p2.lat, p2.lng);
            }
        }
        
        // 只输出routing length和time
        System.out.printf("Average routing length: %.2f%n", totalRoutingLength / numQueries);
        System.out.printf("Average routing time (distance): %.2f%n", totalRoutingTime / numQueries);
    }
    
    private void testSimpleDHTRangePerformance(List<List<TrajPoint>> trajectories,
                                             double globalMinLat, double globalMaxLat,
                                             double globalMinLng, double globalMaxLng,
                                             int rangeFactor) {
        double totalRoutingLength = 0;
        double totalRoutingTime = 0;
        int numQueries = 10;
        
        // 计算查询范围大小
        double latRange = (globalMaxLat - globalMinLat) / rangeFactor;
        double lngRange = (globalMaxLng - globalMinLng) / rangeFactor;
        
        for (int i = 0; i < numQueries; i++) {
            // 随机生成查询中心点
            double centerLat = globalMinLat + random.nextDouble() * (globalMaxLat - globalMinLat);
            double centerLng = globalMinLng + random.nextDouble() * (globalMaxLng - globalMinLng);
            
            // 计算查询范围
            double lat1 = Math.max(centerLat - latRange/2, globalMinLat);
            double lat2 = Math.min(centerLat + latRange/2, globalMaxLat);
            double lng1 = Math.max(centerLng - lngRange/2, globalMinLng);
            double lng2 = Math.min(centerLng + lngRange/2, globalMaxLng);
            
            long t1 = 1176341492L;
            long t2 = 1343349080L;
            
            QueryResult<Object> result = simpleDHT.queryRange(lat1, lat2, lng1, lng2, t1, t2);
            
            totalRoutingLength += result.getRoutingLength();
            
            // 计算路由路径的总距离
            List<DHTNode> path = result.getPath();
            for (int j = 1; j < path.size(); j++) {
                Point p1 = nodeLocations.get(path.get(j-1).getNodeId());
                Point p2 = nodeLocations.get(path.get(j).getNodeId());
                totalRoutingTime += calculateDistance(p1.lat, p1.lng, p2.lat, p2.lng);
            }
        }
        
        // 只输出routing length和time
        System.out.printf("Average routing length: %.2f%n", totalRoutingLength / numQueries);
        System.out.printf("Average routing time (distance): %.2f%n", totalRoutingTime / numQueries);
    }
    
    private String generateKey(TrajPoint point) {
        // 组合 trajId, timestamp, lat, lng 生成key
        return String.format("%d:%d:%.6f:%.6f", 
            point.getTrajId(),
            point.getTimestamp(),
            point.getOriLat(),
            point.getOriLng());
    }
    
    private void printRoutingStatistics(Map<Integer, Integer> routingLengths, 
                                      Map<String, Integer> nodeHits,
                                      double totalRoutingTime,
                                      int numTrajectories) {
        // 计算平均路由长度
        double avgRoutingLength = routingLengths.entrySet().stream()
            .mapToDouble(e -> e.getKey() * e.getValue())
            .sum() / routingLengths.values().stream().mapToInt(Integer::intValue).sum();
        
        System.out.printf("Average routing length: %.2f%n", avgRoutingLength);
        
        // 计算每个节点的命中率
        // int totalHits = nodeHits.values().stream().mapToInt(Integer::intValue).sum();
        // nodeHits.forEach((prefix, hits) -> {
        //     double hitRatio = (double) hits / totalHits;
        //     System.out.printf("Node %s hit ratio: %.4f%n", prefix, hitRatio);
        // });
        
        // 打印总路由时间
        System.out.printf("Total routing time (distance): %.2f%n", totalRoutingTime);
        System.out.printf("Average routing time per trajectory: %.2f%n", 
            totalRoutingTime / numTrajectories);
    }
} 