package org.example.exp.dht;

import java.util.*;
import java.math.BigInteger;

public class SimpleDHT {
    private List<DHTNode> nodes;
    private static final int SHA_BITS = 160;
    private final Random random = new Random();

    public SimpleDHT(int numNodes) {
        this.nodes = new ArrayList<>();
        
        // Initialize nodes
        for (int i = 0; i < numNodes; i++) {
            String nodeId = DHTUtils.sha256("node_" + i);
            nodes.add(new DHTNode(nodeId));
        }
        
        // Sort nodes by ID
        nodes.sort(Comparator.comparing(DHTNode::getNodeId));
        
        // Build finger tables
        buildFingerTables();
    }

    private void buildFingerTables() {
        for (DHTNode node : nodes) {
            List<DHTNode> fingerTable = new ArrayList<>();
            for (int i = 0; i < SHA_BITS; i++) {
                BigInteger nodeId = new BigInteger(node.getNodeId(), 16);
                BigInteger fingerPos = nodeId.add(BigInteger.valueOf(2).pow(i))
                    .mod(BigInteger.valueOf(2).pow(SHA_BITS));
                String fingerHex = fingerPos.toString(16);
                
                DHTNode responsible = findResponsibleNode(fingerHex);
                fingerTable.add(responsible);
            }
            node.setFingerTable(fingerTable);
        }
    }

    private boolean isResponsible(DHTNode node, String keyHash) {
        int nodeIdx = nodes.indexOf(node);
        DHTNode prevNode = nodes.get((nodeIdx - 1 + nodes.size()) % nodes.size());
        
        if (nodeIdx == 0) {
            return keyHash.compareTo(nodes.get(nodes.size() - 1).getNodeId()) > 0 ||
                   keyHash.compareTo(node.getNodeId()) <= 0;
        }
        
        return keyHash.compareTo(prevNode.getNodeId()) > 0 &&
               keyHash.compareTo(node.getNodeId()) <= 0;
    }

    private DHTNode findNextHop(DHTNode currentNode, String keyHash) {
        if (isResponsible(currentNode, keyHash)) {
            return currentNode;
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
        
        if (closestNode == currentNode && !currentNode.getFingerTable().isEmpty()) {
            return currentNode.getFingerTable().get(0);
        }
        return closestNode;
    }

    private BigInteger calculateDistance(String nodeId1, String nodeId2) {
        BigInteger n1 = new BigInteger(nodeId1, 16);
        BigInteger n2 = new BigInteger(nodeId2, 16);
        BigInteger maxHash = BigInteger.valueOf(2).pow(SHA_BITS);
        BigInteger directDistance = n1.subtract(n2).abs();
        BigInteger wrapDistance = maxHash.subtract(directDistance);
        return directDistance.min(wrapDistance);
    }

    private DHTNode findResponsibleNode(String keyHash) {
        for (DHTNode node : nodes) {
            if (isResponsible(node, keyHash)) {
                return node;
            }
        }
        return nodes.get(0);
    }

    public DHTRoutingResult lookup(String key) {
        String keyHash = DHTUtils.sha256(key);
        DHTNode startNode = nodes.get(random.nextInt(nodes.size()));
        return routeToNode(startNode, keyHash);
    }

    private DHTRoutingResult routeToNode(DHTNode startNode, String keyHash) {
        DHTNode currentNode = startNode;
        List<DHTNode> path = new ArrayList<>();
        Set<DHTNode> visited = new HashSet<>();
        
        path.add(currentNode);
        visited.add(currentNode);
        
        int maxSteps = nodes.size();
        int steps = 0;
        
        while (steps < maxSteps) {
            currentNode.incrementHitCount();
            
            if (isResponsible(currentNode, keyHash)) {
                return new DHTRoutingResult(currentNode, path);
            }
            
            DHTNode nextNode = findNextHop(currentNode, keyHash);
            
            if (nextNode == currentNode || visited.contains(nextNode)) {
                break;
            }
            
            currentNode = nextNode;
            path.add(currentNode);
            visited.add(currentNode);
            steps++;
        }
        
        DHTNode responsibleNode = findResponsibleNode(keyHash);
        if (!path.contains(responsibleNode)) {
            path.add(responsibleNode);
            responsibleNode.incrementHitCount();
        }
        
        return new DHTRoutingResult(responsibleNode, path);
    }

    public QueryResult<Object> queryRange(double lat1, double lat2, double lon1, double lon2,
                                        long t1, long t2) {
        List<Object> results = new ArrayList<>();
        List<DHTNode> allPaths = new ArrayList<>();
        Set<DHTNode> accessedNodes = new HashSet<>();
        
        // Generate boundary points for the query range
        List<Point> boundaryPoints = Arrays.asList(
            new Point(lat1, lon1, t1),
            new Point(lat1, lon2, t1),
            new Point(lat2, lon1, t1),
            new Point(lat2, lon2, t1),
            new Point(lat1, lon1, t2),
            new Point(lat1, lon2, t2),
            new Point(lat2, lon1, t2),
            new Point(lat2, lon2, t2)
        );
        
        // Start from a random node
        DHTNode startNode = nodes.get(random.nextInt(nodes.size()));
        
        // Route to each boundary point
        for (Point point : boundaryPoints) {
            String keyHash = DHTUtils.sha256(point.toString());
            DHTRoutingResult result = routeToNode(startNode, keyHash);
            allPaths.addAll(result.getPath());
            accessedNodes.add(result.getNode());
        }
        
        // Check data in all accessed nodes
        for (DHTNode node : accessedNodes) {
            for (Object[] data : node.getData()) {
                Point point = (Point) data[0];
                if (point.lat >= lat1 && point.lat <= lat2 &&
                    point.lng >= lon1 && point.lng <= lon2 &&
                    point.timestamp >= t1 && point.timestamp <= t2) {
                    results.add(data[1]);
                }
            }
        }
        
        // Calculate hit ratio
        int totalHits = nodes.stream().mapToInt(DHTNode::getHitCount).sum();
        double hitRatio = totalHits > 0 ? 
            (double) accessedNodes.stream().mapToInt(DHTNode::getHitCount).sum() / totalHits : 0;
        
        return new QueryResult<>(results, allPaths.size(), hitRatio, allPaths);
    }

    public void insert(String key, Object value) {
        DHTRoutingResult result = lookup(key);
        DHTNode targetNode = result.getNode();
        targetNode.getData().add(new Object[]{key, value});
    }

    private static class Point {
        double lat, lng;
        long timestamp;
        
        Point(double lat, double lng, long timestamp) {
            this.lat = lat;
            this.lng = lng;
            this.timestamp = timestamp;
        }
        
        @Override
        public String toString() {
            return String.format("%f:%f:%d", lat, lng, timestamp);
        }
    }
} 