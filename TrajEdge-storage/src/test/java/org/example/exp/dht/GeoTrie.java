package org.example.exp.dht;

import java.util.*;
import java.math.BigInteger;
import java.util.stream.Collectors;

public class GeoTrie {
    private Map<String, DHTNode> nodes;
    private DHTNode root;
    private final int numBits;
    private final int numBitsTree;
    private final int splitThreshold;
    private static final int SHA_BITS = 160;
    private static final Random RANDOM = new Random(12345);

    public GeoTrie(int numNodes, int numBits) {
        this.numBits = numBits;
        this.numBitsTree = numBits;
        this.splitThreshold = 10;
        this.nodes = new HashMap<>();

        // Initialize nodes
        for (int i = 0; i < numNodes; i++) {
            String nodeId = DHTUtils.sha256("node_" + i);
            nodes.put(nodeId, new DHTNode(nodeId));
        }

        // Set root node
        this.root = nodes.values().iterator().next();
        this.root.setType("leaf");

        buildFingerTables();
    }

    private void buildFingerTables() {
        for (DHTNode node : nodes.values()) {
            List<DHTNode> fingerTable = new ArrayList<>();
            BigInteger nodeId = new BigInteger(node.getNodeId(), 16);
            BigInteger maxValue = BigInteger.valueOf(2).pow(SHA_BITS);
            
            for (int i = 0; i < SHA_BITS; i++) {
                // 计算理想的finger位置
                BigInteger fingerPos = nodeId.add(BigInteger.valueOf(2).pow(i))
                                           .mod(maxValue);
                String fingerHex = fingerPos.toString(16);
                while (fingerHex.length() < 40) {  // 确保是40位十六进制
                    fingerHex = "0" + fingerHex;
                }
                
                // 找到负责该位置的节点
                DHTNode responsible = findResponsibleNode(fingerHex);
                fingerTable.add(responsible);
            }
            node.setFingerTable(fingerTable);
        }
    }

    private DHTNode findResponsibleNode(String keyHash) {
        List<DHTNode> sortedNodes = new ArrayList<>(nodes.values());
        sortedNodes.sort(Comparator.comparing(DHTNode::getNodeId));
        
        for (int i = 0; i < sortedNodes.size(); i++) {
            DHTNode node = sortedNodes.get(i);
            if (i == sortedNodes.size() - 1) {
                return sortedNodes.get(0);
            }
            if (node.getNodeId().compareTo(keyHash) <= 0 && 
                sortedNodes.get(i + 1).getNodeId().compareTo(keyHash) > 0) {
                return node;
            }
        }
        return sortedNodes.get(0);
    }

    private String[] encodeGpsData(double latitude, double longitude, long timestamp) {
        String Tlat = DHTUtils.encodeCoordinate(latitude, -90, 90, numBitsTree);
        String Tlon = DHTUtils.encodeCoordinate(longitude, -180, 180, numBitsTree);
        String Tt = String.format("%" + numBitsTree + "s", 
            Long.toBinaryString(timestamp)).replace(' ', '0');
        return new String[]{Tlat, Tlon, Tt};
    }

    private String getPrefix(String[] Tk, int length) {
        if (length <= 0) {
            return "*";
        }
        StringBuilder prefix = new StringBuilder();
        for (String t : Tk) {
            if (length <= t.length()) {
                prefix.append(t.substring(0, length));
            }
        }
        return prefix.toString();
    }

    private DHTRoutingResult dhtLookup(String prefix) {
        // 从随机节点开始DHT路由
        DHTNode startNode = getRandomStartNode();
        List<DHTNode> path = new ArrayList<>();
        Set<DHTNode> visited = new HashSet<>();
        DHTNode currentNode = startNode;
        
        path.add(startNode);
        visited.add(startNode);

        // 计算目标hash
        String keyHash;
        if ("*".equals(prefix)) {
            // 对于"*"，生成一个随机hash值作为目标
            byte[] randomBytes = new byte[20]; // SHA-1 hash长度为160位(20字节)
            RANDOM.nextBytes(randomBytes);
            StringBuilder sb = new StringBuilder();
            for (byte b : randomBytes) {
                sb.append(String.format("%02x", b));
            }
            keyHash = sb.toString();
        } else {
            keyHash = DHTUtils.sha256(prefix);
        }

        int maxSteps = nodes.size();
        int steps = 0;

        while (steps < maxSteps) {
            // 每次访问节点时增加计数
            currentNode.incrementHitCount();
            
            // 如果当前节点负责该key，结束路由
            if (isResponsible(currentNode, keyHash)) {
                return new DHTRoutingResult(currentNode, path);
            }

            // 找到下一跳节点
            DHTNode nextNode = findNextHop(currentNode, keyHash);

            // 如果下一跳是当前节点或已访问过的节点，说明找不到更好的路径
            if (nextNode == currentNode || visited.contains(nextNode)) {
                break;
            }

            currentNode = nextNode;
            path.add(currentNode);
            visited.add(currentNode);
            steps++;
        }

        // 如果路由结束但未找到负责节点，返回负责该key的节点
        DHTNode responsibleNode = findResponsibleNode(keyHash);
        if (!path.contains(responsibleNode)) {
            path.add(responsibleNode);
            responsibleNode.incrementHitCount();
        }

        return new DHTRoutingResult(responsibleNode, path);
    }

    private boolean isResponsible(DHTNode node, String keyHash) {
        List<DHTNode> sortedNodes = new ArrayList<>(nodes.values());
        sortedNodes.sort(Comparator.comparing(DHTNode::getNodeId));
        int nodeIdx = sortedNodes.indexOf(node);
        DHTNode prevNode = sortedNodes.get((nodeIdx - 1 + sortedNodes.size()) % sortedNodes.size());

        return (prevNode.getNodeId().compareTo(keyHash) < 0 && 
                node.getNodeId().compareTo(keyHash) >= 0) ||
               (nodeIdx == 0 && keyHash.compareTo(sortedNodes.get(sortedNodes.size() - 1).getNodeId()) > 0);
    }

    private DHTNode findNextHop(DHTNode currentNode, String keyHash) {
        if (isResponsible(currentNode, keyHash)) {
            return currentNode;
        }

        if (currentNode.getFingerTable().isEmpty()) {
            buildFingerTables();
        }

        DHTNode closestNode = currentNode;
        BigInteger minDistance = calculateDistance(currentNode.getNodeId(), keyHash);

        for (DHTNode fingerNode : currentNode.getFingerTable()) {
            BigInteger distance = calculateDistance(fingerNode.getNodeId(), keyHash);
            if (distance.compareTo(minDistance) < 0) {
                minDistance = distance;
                closestNode = fingerNode;
            }
        }

        // 如果找不到更近的节点，返回第一个finger节点
        if (closestNode == currentNode && !currentNode.getFingerTable().isEmpty()) {
            return currentNode.getFingerTable().get(0);
        }
        return closestNode;
    }

    private BigInteger calculateDistance(String nodeId1, String nodeId2) {
        BigInteger n1 = new BigInteger(nodeId1, 16);
        BigInteger n2 = new BigInteger(nodeId2, 16);
        BigInteger maxHash = BigInteger.valueOf(2).pow(160);
        BigInteger directDistance = n1.subtract(n2).abs();
        BigInteger wrapDistance = maxHash.subtract(directDistance);
        return directDistance.min(wrapDistance);
    }

    private void splitNode(DHTNode node, String prefix, boolean forceRootSplit) {
        if (node.getData().isEmpty()) {
            return;
        }

        // 如果是根节点且强制分裂，或者是其他节点且数据超过阈值时分裂
        if ((node == root && forceRootSplit) || 
            (node != root && node.getData().size() > splitThreshold)) {
            
            // Mark current node as internal
            node.setType("internal");

            // 计算当前分裂位置
            int bitsPerDimension = numBits / 3;
            int bitPosition;
            if ("*".equals(prefix)) {
                bitPosition = 0;
            } else {
                bitPosition = (prefix.length() / 3) % bitsPerDimension;
            }
            
            // 只有当位置小于每个维度的最大位数时才分裂
            if (bitPosition < bitsPerDimension) {
                // 创建所有8个子节点的bit组合 (2^3 = 8)
                List<String> allBits = Arrays.asList(
                    "000", "001", "010", "011",
                    "100", "101", "110", "111"
                );

                // 获取可用节点
                List<DHTNode> availableNodes = nodes.values().stream()
                    .filter(n -> !node.getChildren().containsValue(n) && n != root)
                    .collect(Collectors.toList());

                // 为每个bit组合创建子节点
                for (String bit : allBits) {
                    if (!availableNodes.isEmpty()) {
                        DHTNode newNode = availableNodes.remove(0);
                        newNode.setType("leaf");
                        newNode.getData().clear();
                        node.getChildren().put(bit, newNode);
                    }
                }

                // 重新分配数据到子节点
                for (Object[] dataEntry : node.getData()) {
                    String[] key = (String[]) dataEntry[0];
                    String currentBit = String.valueOf(key[0].charAt(bitPosition)) +
                                      String.valueOf(key[1].charAt(bitPosition)) +
                                      String.valueOf(key[2].charAt(bitPosition));
                    
                    if (node.getChildren().containsKey(currentBit)) {
                        DHTNode child = node.getChildren().get(currentBit);
                        child.getData().add(dataEntry);
                        
                        // 检查子节点是否需要继续分裂
                        if (child.getData().size() > splitThreshold) {
                            splitNode(child, prefix + currentBit, false);
                        }
                    }
                }

                // Clear current node's data
                node.getData().clear();
            }
        }
    }

    private LeafLocatorResult locateLeaf(String[] Tk) {
        List<DHTNode> allPath = new ArrayList<>();
        int lower = 0;
        int higher = numBits;
        DHTNode currentNode = root;
        allPath.add(currentNode);
        
        while (lower < higher) {
            int middle = (lower + higher) / 2;
            String prefix = getPrefix(Tk, middle);
            
            // DHT查找，获取节点和路由路径
            DHTRoutingResult routingResult = dhtLookup(prefix);
            allPath.addAll(routingResult.getPath());
            DHTNode node = routingResult.getNode();
            
            if ("leaf".equals(node.getType())) {
                // 检查是否需要分裂
                if (node.getData().size() > splitThreshold) {
                    splitNode(node, prefix, true);
                    // 如果节点被分裂，继续查找对应的子节点
                    if (middle < Tk[0].length()) {
                        String bit = String.valueOf(Tk[0].charAt(middle)) +
                                   String.valueOf(Tk[1].charAt(middle)) +
                                   String.valueOf(Tk[2].charAt(middle));
                        if (node.getChildren().containsKey(bit)) {
                            return new LeafLocatorResult(node.getChildren().get(bit), allPath);
                        }
                    }
                }
                return new LeafLocatorResult(node, allPath);
            } else if ("internal".equals(node.getType())) {
                // 查找对应的子节点
                if (middle < Tk[0].length()) {
                    String bit = String.valueOf(Tk[0].charAt(middle)) +
                               String.valueOf(Tk[1].charAt(middle)) +
                               String.valueOf(Tk[2].charAt(middle));
                    if (node.getChildren().containsKey(bit)) {
                        currentNode = node.getChildren().get(bit);
                        allPath.add(currentNode);
                        if ("leaf".equals(currentNode.getType())) {
                            return new LeafLocatorResult(currentNode, allPath);
                        }
                    }
                }
                lower = middle + 1;
            } else { // external
                higher = middle - 1;
            }
        }
        
        // 如果二分查找失败，使用最小的有效前缀长度
        int prefixLength = Math.max(0, higher);
        String prefix = getPrefix(Tk, prefixLength);
        DHTRoutingResult routingResult = dhtLookup(prefix);
        allPath.addAll(routingResult.getPath());
        DHTNode node = routingResult.getNode();
        
        if ("external".equals(node.getType())) {
            node.setType("leaf");
        }
        
        return new LeafLocatorResult(node, allPath);
    }

    // Helper class for DHT routing results
    private static class DHTRoutingResult {
        private final DHTNode node;
        private final List<DHTNode> path;

        public DHTRoutingResult(DHTNode node, List<DHTNode> path) {
            this.node = node;
            this.path = path;
        }

        public DHTNode getNode() { return node; }
        public List<DHTNode> getPath() { return path; }
    }

    // Helper class for leaf locator results
    private static class LeafLocatorResult {
        private final DHTNode node;
        private final List<DHTNode> path;

        public LeafLocatorResult(DHTNode node, List<DHTNode> path) {
            this.node = node;
            this.path = path;
        }

        public DHTNode getNode() { return node; }
        public List<DHTNode> getPath() { return path; }
    }

    public QueryResult<Object> insert(double latitude, double longitude, long timestamp, Object metaData) {
        // Encode GPS data
        String[] Tk = encodeGpsData(latitude, longitude, timestamp);
        
        // 从root开始
        List<DHTNode> path = new ArrayList<>();
        path.add(root);
        
        // 先插入root
        root.getData().add(new Object[]{Tk, metaData});
        
        // 检查是否需要分裂，即使数据没满也强制分裂根节点
        if (root.getType().equals("leaf")) {
            splitNode(root, "*", true); // 添加一个强制分裂参数
        }
        
        // 如果已经分裂，找到对应的叶子节点
        if (root.getType().equals("internal")) {
            LeafLocatorResult result = locateLeaf(Tk);
            path.addAll(result.getPath());
        }
        
        // Calculate routing length and hit ratio
        int routingLength = path.size();
        long totalHits = nodes.values().stream()
            .mapToLong(DHTNode::getHitCount)
            .sum();
        
        double hitRatio = totalHits > 0 ? 
            path.stream().mapToLong(DHTNode::getHitCount).sum() / (double) totalHits : 0;
        
        return new QueryResult<>(Collections.singletonList(metaData), routingLength, hitRatio, path);
    }

    private String findCommonPrefix(String range1, String range2) {
        StringBuilder common = new StringBuilder();
        int minLength = Math.min(range1.length(), range2.length());
        
        for (int i = 0; i < minLength; i++) {
            if (range1.charAt(i) != range2.charAt(i)) {
                break;
            }
            common.append(range1.charAt(i));
        }
        
        return common.toString();
    }

    public QueryResult<Object> queryRange(double lat1, double lat2, double lon1, double lon2, 
                                        long t1, long t2) {
        // 编码范围边界
        String[] rangeStart = encodeGpsData(lat1, lon1, t1);
        String[] rangeEnd = encodeGpsData(lat2, lon2, t2);
        
        // 找到每个维度的公共前缀
        String prefixLat = findCommonPrefix(rangeStart[0], rangeEnd[0]);
        String prefixLon = findCommonPrefix(rangeStart[1], rangeEnd[1]);
        String prefixTime = findCommonPrefix(rangeStart[2], rangeEnd[2]);
        
        // 使用最短的公共前缀
        int minPrefixLen = Math.min(Math.min(prefixLat.length(), prefixLon.length()), 
                                   prefixTime.length());
        String target = minPrefixLen == 0 ? "*" : getPrefix(rangeStart, minPrefixLen);
        
        // 从目标节点开始查询
        DHTRoutingResult routingResult = dhtLookup(target);
        List<Object> results = new ArrayList<>();
        Set<DHTNode> accessedNodes = new HashSet<>();
        List<DHTNode> allPaths = new ArrayList<>(routingResult.getPath());
        
        // 处理查询
        processNode(routingResult.getNode(), rangeStart, rangeEnd, results, accessedNodes, allPaths);
        
        // 计算路由长度和命中率
        int routingLength = allPaths.size();
        long totalHits = nodes.values().stream()
            .mapToLong(DHTNode::getHitCount)
            .sum();
        
        double hitRatio = totalHits > 0 ? 
            accessedNodes.stream().mapToLong(DHTNode::getHitCount).sum() / (double) totalHits : 0;
        
        return new QueryResult<>(results, routingLength, hitRatio, allPaths);
    }

    private void processNode(DHTNode node, String[] rangeStart, String[] rangeEnd,
                            List<Object> results, Set<DHTNode> accessedNodes, List<DHTNode> allPaths) {
        if (accessedNodes.contains(node)) {
            return;
        }
        
        accessedNodes.add(node);
        node.incrementHitCount();
        
        if ("leaf".equals(node.getType())) {
            for (Object[] dataEntry : node.getData()) {
                String[] key = (String[]) dataEntry[0];
                if (isInRange(key, rangeStart, rangeEnd)) {
                    results.add(dataEntry[1]);
                }
            }
        } else if ("internal".equals(node.getType())) {
            for (DHTNode child : node.getChildren().values()) {
                if (!accessedNodes.contains(child)) {
                    processNode(child, rangeStart, rangeEnd, results, accessedNodes, allPaths);
                }
            }
        } else { // external
            LeafLocatorResult result = locateLeaf(rangeStart);
            allPaths.addAll(result.getPath());
            result.getPath().stream()
                .filter(n -> !allPaths.contains(n))
                .forEach(allPaths::add);
            processNode(result.getNode(), rangeStart, rangeEnd, results, accessedNodes, allPaths);
        }
    }

    private boolean isInRange(String[] key, String[] rangeStart, String[] rangeEnd) {
        // 确保范围的起始值小于结束值
        String[] actualStart = new String[3];
        String[] actualEnd = new String[3];
        
        for (int i = 0; i < 3; i++) {
            if (rangeStart[i].compareTo(rangeEnd[i]) <= 0) {
                actualStart[i] = rangeStart[i];
                actualEnd[i] = rangeEnd[i];
            } else {
                actualStart[i] = rangeEnd[i];
                actualEnd[i] = rangeStart[i];
            }
        }
        
        return key[0].compareTo(actualStart[0]) >= 0 && key[0].compareTo(actualEnd[0]) <= 0 &&
               key[1].compareTo(actualStart[1]) >= 0 && key[1].compareTo(actualEnd[1]) <= 0 &&
               key[2].compareTo(actualStart[2]) >= 0 && key[2].compareTo(actualEnd[2]) <= 0;
    }

    private DHTNode getRandomStartNode() {
        DHTNode[] nodeArray = nodes.values().toArray(new DHTNode[0]);
        return nodeArray[RANDOM.nextInt(nodeArray.length)];
    }

    public static void main(String[] args) {
        // Create GeoTrie instance
        GeoTrie geoTrie = new GeoTrie(30, 32);
        
        // Test insertions
        System.out.println("Testing insertions...");
        Random random = new Random(12345);
        List<double[]> insertedData = new ArrayList<>();
        
        for (int i = 0; i < 100; i++) {
            double[] data = new double[] {
                random.nextDouble() * 180 - 90,     // latitude [-90, 90]
                random.nextDouble() * 360 - 180,    // longitude [-180, 180]
                random.nextInt(Integer.MAX_VALUE)   // timestamp
            };
            insertedData.add(data);
            
            QueryResult<Object> result = geoTrie.insert(data[0], data[1], (long)data[2], data);
            System.out.printf("Insert: Routing length = %d, Hit ratio = %.4f%n", 
                result.getRoutingLength(), result.getHitRatio());
        }
        
        // Test range query
        System.out.println("\nTesting range query...");
        double[] firstData = insertedData.get(0);
        QueryResult<Object> result = geoTrie.queryRange(
            firstData[0], firstData[0] - 1,
            firstData[1], firstData[1] + 1,
            (long)firstData[2] - 100, (long)firstData[2] + 100
        );
        
        System.out.printf("Range Query: Routing length = %d, Hit ratio = %.4f%n", 
            result.getRoutingLength(), result.getHitRatio());
        System.out.printf("Number of results: %d%n", result.getResults().size());
    }
} 