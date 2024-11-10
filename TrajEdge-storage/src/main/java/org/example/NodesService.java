package org.example;

import java.util.List;

import org.example.trajstore.TrajPoint;
import java.util.ArrayList;
import java.util.Comparator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import org.example.grpc.TrajectoryServiceGrpc;
import org.example.grpc.TrajectoryRequest;
import org.example.grpc.TrajectoryResponse;
import org.example.grpc.TrajectoryPoint;
import java.util.TreeMap;

import org.example.grpc.QueryByPrefixRequest;
import org.example.struct.KeyRange;
import org.example.struct.Key;
import org.example.struct.Node;
import org.example.struct.STHTIndex;
import java.util.Random;
import org.apache.commons.lang3.tuple.Pair; 


public class NodesService extends TrajectoryServiceGrpc.TrajectoryServiceImplBase{
    private static final Logger LOG = LoggerFactory.getLogger(NodesService.class);
    private static final String supervisor = "supervisor-";
    private Map<String, Map<String, String>> nodeInfos;
    private Map<String, Node> nodes;
    private STHTIndex indexUtils;
    private final String dockerName;
    private final Integer port;
    private String id;
    private String confPath = "/data/conf";
    private Boolean debug;


    public NodesService(String dockerName, int port, boolean debug){
        this.nodeInfos = new TreeMap<>();
        this.nodes = new TreeMap<>();
        this.indexUtils = new STHTIndex();
        this.dockerName = dockerName;
        this.port = port;
        this.debug = debug;
        if(debug){
            this.confPath = "/home/hch/PROJECT/TrajEdge/conf";
            this.id = "6";
        }
        else{
            // Load configuration file
            this.id = dockerName.split("-")[1];
        }
        loadConfigFiles(id);
        initNodes();
    }

    
    @Override
    public void addTrajectoryData(TrajectoryRequest request, StreamObserver<TrajectoryResponse> responseObserver) {
        List<TrajPoint> trajectory = convertToTrajPoints(request.getPointsList());

        if (trajectory == null || trajectory.isEmpty()) {
            return;
        }
        String nextDockerName = "";

        try{
            String key = indexUtils.encodeUniversalKey(trajectory);
            String nodePrefix = NodesService.longestPrefixMatch(key, nodes.keySet().toArray(new String[0]));
            while(true){
                Node node = nodes.get(nodePrefix);
                String[] result = node.insert(key, trajectory);
                String nextPrefix = result[0];
                nextDockerName = result[1];
                // 插入成功或者不在当前docker
                if(nextPrefix.isEmpty() || !nodes.containsKey(nextPrefix)){
                    if(nextPrefix.isEmpty()) LOG.info(dockerName +" 插入 " + key + " 成功");
                    else if(!nodes.containsKey(nextPrefix)) LOG.info("当前docker {}, {} 不在当前docker, 路由到docker {} 中的 {}.", dockerName, key, nextDockerName, nextPrefix);
                    break;
                }
                nodePrefix = nextPrefix;
            }
        }
        catch(Exception e){
            LOG.error("Insert trajectory error. ", trajectory);
            e.printStackTrace();
        }
        if(!nextDockerName.isEmpty()) nextDockerName = supervisor + nextDockerName;
        
        TrajectoryResponse response = TrajectoryResponse.newBuilder()
                .setNextNodeId(nextDockerName)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void readTrajectoryData(TrajectoryRequest request, StreamObserver<TrajectoryResponse> responseObserver) {
        List<TrajPoint> trajPoints = new ArrayList<>();
        int queryType = request.getQueryType();
        int trajId = request.getTrajId();
        int topk = request.getTopk();

        // 1.先生成需要查询的key ranges
        List<KeyRange> keyRanges = indexUtils.gKeyRanges(request.getStartTime(), request.getEndTime(),
        request.getMinLat(), request.getMaxLat(), request.getMinLng(), request.getMaxLng());
        // 2.对于每个key range执行一次查询
        for(KeyRange range : keyRanges){
            // 3.计算key range的起始key和终止key的最长公共前缀,找到对应的node
            Iterator<Key> iter = range.iterator();
            while(iter.hasNext()){
                String prefix = iter.next().getKey();
                trajPoints.addAll(internalFind(prefix, trajId, request.getStartTime(), request.getEndTime(),
                request.getMinLat(), request.getMaxLat(), request.getMinLng(), request.getMaxLng()));
            }
        }
        // kNN query
        if(queryType == 4){
            double midLat = (request.getMinLat() + request.getMaxLat()) / 2;
            double midLng = (request.getMinLat() + request.getMaxLng()) / 2;
            // Create a list to store distances and corresponding points
            List<Pair<TrajPoint, Double>> distanceList = new ArrayList<>();
            for (TrajPoint point : trajPoints) {
                double distance = calculateDistance(midLat, midLng, point.getOriLat(), point.getOriLng());
                distanceList.add(Pair.of(point, distance));
            }

            // Sort the list by distance
            distanceList.sort(Comparator.comparingDouble(Pair::getValue));

            // Retrieve the top k points
            trajPoints.clear(); // Clear existing points
            for (int i = 0; i < Math.min(topk, distanceList.size()); i++) {
                trajPoints.add(distanceList.get(i).getKey());
            }
        }
                
        TrajectoryResponse.Builder responseBuilder = TrajectoryResponse.newBuilder();
        for (TrajPoint point : trajPoints) {
            responseBuilder.addPoints(convertToTrajectoryPoint(point));
        }

        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }


    @Override
    public void queryByPrefix(QueryByPrefixRequest request, StreamObserver<TrajectoryResponse> responseObserver) {
        String targetPrefix = request.getPrefix();
        Integer trajId = request.getTrajId();
        List<TrajPoint> points = internalFind(targetPrefix, trajId, request.getStartTime(), request.getEndTime(),
        request.getMinLat(), request.getMaxLat(), request.getMinLng(), request.getMaxLng());

        TrajectoryResponse.Builder responseBuilder = TrajectoryResponse.newBuilder();
        for (TrajPoint point : points) {
            responseBuilder.addPoints(convertToTrajectoryPoint(point));
        }

        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    private void initNodes(){
        for(String prefix : nodeInfos.keySet()){
            nodes.put(prefix, new Node(prefix, nodeInfos.get(prefix), dockerName));
        }
    }

    private void loadConfigFiles(String id) {
        // Load allocate files
        loadFile("allocate_" + id + ".txt", "allocate");
        loadFile("children_" + id + ".txt", "children");
        loadFile("neighbor_" + id + ".txt", "neighbor");
        loadFile("parent_" + id + ".txt", "parent");
    }

    private void loadFile(String fileName, String type) {
        String filePath = confPath + "/" + fileName;
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) continue;
                
                if (type.equals("allocate")) {
                    // For allocate files, just store the prefix
                    nodeInfos.put(line.trim(), new HashMap<>());
                } else {
                    // For other files, parse the relationship information
                    parseRelationship(line, type);
                }
            }
        } catch (IOException e) {
            LOG.error("Error reading file: " + fileName, e);
        }
    }

    private void parseRelationship(String line, String type) {
        // Skip empty lines or comments
        if (line.trim().isEmpty() || !line.contains(",")) {
            return;
        }

        // Split the line into parts
        String[] parts = line.split(",", 2);
        String prefix = parts[0].trim();
        String relationInfo = parts[1].trim();
        String[] relatedNodes = relationInfo.split(",");
        for(String relatedNode : relatedNodes){
            String[] info = relatedNode.split(":");
            String relatedPrefix = info[0].trim();
            String dockerId = info[1].trim();
            // Store the relationship information
            nodeInfos.get(prefix).put(relatedPrefix, dockerId);
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

    public static String longestPrefixMatch(String s, String[] S) {
        String longestMatch = "";
        String oriPrefix = "????";
        for (String str : S) {
            // Check if str is a prefix of s
            if (s.startsWith(str) && str.length() >= longestMatch.length()) {
                longestMatch = str;
                oriPrefix = str;
            }
            // Check for partial matches
            int i = 0;
            for (; i < Math.min(s.length(), str.length()); i++) {
                if (s.charAt(i) != str.charAt(i)) {
                    if (i >= longestMatch.length() && str.length() <= oriPrefix.length()) {
                        longestMatch = str.substring(0, i);
                        oriPrefix = str;
                    }
                    break;
                }
            }
            
            if(i == Math.min(s.length(), str.length()) && i >= longestMatch.length() && str.length() <= oriPrefix.length()){
                longestMatch = str.substring(0, i);
                oriPrefix = str;
            }
        }
        
        // Return the original string from S that has the longest prefix match
        if (!oriPrefix.isEmpty()) {
            return oriPrefix; // Return the original string
        } else {
            // Return a random string from S if no match found
            Random random = new Random();
            return S[random.nextInt(S.length)];
        }
    }

    private List<TrajPoint> internalFind(String targetPrefix, Integer trajId, long startTime, long endTime, 
    double minLat, double maxLat, double minLng, double maxLng){
        List<TrajPoint> trajPoints = new ArrayList<>();
         // 4.先在本地的节点中找最长匹配的
         String prefix = longestPrefixMatch(targetPrefix, nodes.keySet().toArray(new String[0]));
         // 5.然后向对应的node询问相邻节点是否存在
         Node node_to_find = nodes.get(prefix);
         String[] findResult = node_to_find.findKey(targetPrefix);
         if(findResult[0].isEmpty()){
            LOG.info("Found {} in local storage.", targetPrefix);
            List<TrajPoint> points = node_to_find.doRead(trajId, startTime, endTime,
            minLat, maxLat, minLng, maxLng);
            trajPoints.addAll(points);
         }
         else{
             String nextDockerId = findResult[1];
             LOG.info("In {}, routing to node {}, skip in debug mode", dockerName, nextDockerId);
             if(this.debug)return trajPoints;
             // 6.通过rpc向nextDockerId对应的server发起远程查询
             String remoteAddress = supervisor + nextDockerId + ":" + port;
             List<TrajPoint> remotePoints = prefixQueryClient(remoteAddress, targetPrefix, 
             trajId, startTime, endTime,
             minLat, maxLat, minLng, maxLng);
             trajPoints.addAll(remotePoints);
         }
         return trajPoints;
    }

    private List<TrajPoint> prefixQueryClient(String remoteAddress, String prefix, Integer trajId,
                                          long startTime, long endTime,
                                          double minLat, double maxLat,
                                          double minLng, double maxLng) {
        LOG.info("Querying remote node by prefix: " + remoteAddress + ", prefix: " + prefix);
        
        ManagedChannel channel = null;
        try {
            channel = ManagedChannelBuilder.forTarget(remoteAddress)
                .usePlaintext()
                .build();
            TrajectoryServiceGrpc.TrajectoryServiceBlockingStub stub = 
                TrajectoryServiceGrpc.newBlockingStub(channel);

            QueryByPrefixRequest request = QueryByPrefixRequest.newBuilder()
                .setPrefix(prefix)
                .setTrajId(trajId)
                .setStartTime(startTime)
                .setEndTime(endTime)
                .setMinLat(minLat)
                .setMaxLat(maxLat)
                .setMinLng(minLng)
                .setMaxLng(maxLng)
                .build();

            TrajectoryResponse response = stub.queryByPrefix(request);
            return convertToTrajPoints(response.getPointsList());
        } catch (Exception e) {
            LOG.error("Error querying remote node by prefix: " + remoteAddress, e);
            return new ArrayList<>();
        } finally {
            if (channel != null && !channel.isShutdown()) {
                channel.shutdown();
            }
        }
    }

    private double calculateDistance(double lat1, double lng1, double lat2, double lng2) {
        final int R = 6371; // Radius of the Earth in kilometers
        double latDistance = Math.toRadians(lat2 - lat1);
        double lngDistance = Math.toRadians(lng2 - lng1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) +
                   Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                   Math.sin(lngDistance / 2) * Math.sin(lngDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c; // Distance in kilometers
    }
}
