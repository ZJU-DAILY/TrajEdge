// package org.example.bolt;

// import org.apache.storm.task.OutputCollector;
// import org.apache.storm.task.TopologyContext;
// import org.apache.storm.topology.OutputFieldsDeclarer;
// import org.apache.storm.topology.base.BaseRichBolt;
// import org.apache.storm.tuple.Fields;
// import org.apache.storm.tuple.Tuple;
// import org.apache.storm.tuple.Values;
// import org.rocksdb.*;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;

// import java.nio.ByteBuffer;
// import java.util.*;

// public class KnnQueryBolt extends BaseRichBolt {
//     private static final Logger LOG = LoggerFactory.getLogger(KnnQueryBolt.class);
//     private OutputCollector collector;
//     private Map<String, Object> stormConf;
//     private TopologyContext context;

//     private Map<Long, Set<Integer>> edgeToTrajMap;
//     private RocksDB db;
//     private String dbPath;

//     // 新增：边ID到坐标的映射
//     private Map<Long, EdgeCoordinates> edgeCoordinatesMap;

//     @Override
//     public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
//         this.stormConf = topoConf;
//         this.context = context;
//         this.collector = collector;
//         this.edgeToTrajMap = new HashMap<>();
//         this.dbPath = (String) topoConf.get("data.index.dest");
//         this.edgeCoordinatesMap = new HashMap<>();

//         initRocksDB();
//         loadEdgeToTrajMap();
//         loadEdgeCoordinates(); // 新增：加载边坐标信息
//     }

//     private void initRocksDB() {
//         try {
//             Options options = new Options().setCreateIfMissing(true);
//             db = RocksDB.open(options, dbPath);
//         } catch (RocksDBException e) {
//             LOG.error("Failed to initialize RocksDB", e);
//             throw new RuntimeException(e);
//         }
//     }

//     @Override
//     public void execute(Tuple tuple) {
//         if (tuple == null || !tuple.contains("KnnQuery")) {
//             return;
//         }

//         Long queryEdgeId = tuple.getLongByField("edgeId");
//         Double queryDistance = tuple.getDoubleByField("distance");
//         int k = tuple.getIntegerByField("k");

//         // 1. 选择距离最近的top k个路网段
//         List<Long> nearestEdges = findNearestEdges(queryEdgeId, queryDistance, k);

//         // 2. 从路网段中取出对应的轨迹id
//         Set<Integer> candidateTrajIds = new HashSet<>();
//         for (Long edgeId : nearestEdges) {
//             candidateTrajIds.addAll(edgeToTrajMap.getOrDefault(edgeId, Collections.emptySet()));
//         }

//         // 3. 发送候选轨迹id进行存储层查询
//         for (Integer trajId : candidateTrajIds) {
//             collector.emit(new Values(trajId, queryEdgeId, queryDistance));
//         }

//         collector.ack(tuple);
//     }

//     @Override
//     public void declareOutputFields(OutputFieldsDeclarer declarer) {
//         declarer.declare(new Fields("trajId", "queryEdgeId", "queryDistance"));
//     }

//     private List<Long> findNearestEdges(Long queryEdgeId, Double queryDistance, int k) {
//         // 使用优先队列来保存K个最近的路网段
//         PriorityQueue<Map.Entry<Long, Double>> pq = new PriorityQueue<>(k, 
//             (a, b) -> Double.compare(b.getValue(), a.getValue()));

//         EdgeCoordinates queryEdgeCoords = edgeCoordinatesMap.get(queryEdgeId);
//         if (queryEdgeCoords == null) {
//             LOG.error("No coordinates found for edge ID: {}", queryEdgeId);
//             return Collections.emptyList();
//         }

//         for (Long edgeId : edgeToTrajMap.keySet()) {
//             double distance = calculateEdgeDistance(edgeId, queryEdgeId, queryDistance);
//             if (pq.size() < k) {
//                 pq.offer(new AbstractMap.SimpleEntry<>(edgeId, distance));
//             } else if (distance < pq.peek().getValue()) {
//                 pq.poll();
//                 pq.offer(new AbstractMap.SimpleEntry<>(edgeId, distance));
//             }
//         }

//         List<Long> result = new ArrayList<>();
//         while (!pq.isEmpty()) {
//             result.add(pq.poll().getKey());
//         }
//         Collections.reverse(result);
//         return result;
//     }

//     private double calculateEdgeDistance(Long edgeId1, Long edgeId2, Double queryDistance) {
//         EdgeCoordinates coords1 = edgeCoordinatesMap.get(edgeId1);
//         EdgeCoordinates coords2 = edgeCoordinatesMap.get(edgeId2);

//         if (coords1 == null || coords2 == null) {
//             LOG.error("Coordinates not found for edge ID: {} or {}", edgeId1, edgeId2);
//             return Double.MAX_VALUE;
//         }

//         // 计算两条边的中点
//         double midX1 = (coords1.startX + coords1.endX) / 2;
//         double midY1 = (coords1.startY + coords1.endY) / 2;
//         double midX2 = (coords2.startX + coords2.endX) / 2;
//         double midY2 = (coords2.startY + coords2.endY) / 2;

//         // 计算中点之间的欧几里得距离
//         return Math.sqrt(Math.pow(midX1 - midX2, 2) + Math.pow(midY1 - midY2, 2));
//     }

//     private void loadEdgeToTrajMap() {
//         try (RocksIterator iterator = db.newIterator()) {
//             for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
//                 byte[] keyBytes = iterator.key();
//                 byte[] valueBytes = iterator.value();

//                 Long edgeId = Long.parseLong(new String(keyBytes));
//                 Set<Integer> trajIds = deserialize(valueBytes);

//                 edgeToTrajMap.put(edgeId, trajIds);
//             }
//         }
//         LOG.info("Loaded mapping for {} edges", edgeToTrajMap.size());
//     }

//     private Set<Integer> deserialize(byte[] values) {
//         Set<Integer> sets = new HashSet<>();
//         ByteBuffer buffer = ByteBuffer.wrap(values);
//         int length = buffer.getInt();
//         for (int i = 0; i < length; i++) {
//             sets.add(buffer.getInt());
//         }
//         return sets;
//     }

//     private void loadEdgeCoordinates() {
//         // 这里应该实现从配置文件或数据库加载边的坐标信息
//         // 示例实现：
//         // edgeCoordinatesMap.put(1L, new EdgeCoordinates(0.0, 0.0, 1.0, 1.0));
//         // edgeCoordinatesMap.put(2L, new EdgeCoordinates(1.0, 1.0, 2.0, 2.0));
//         // ...
//         LOG.info("Loaded coordinates for {} edges", edgeCoordinatesMap.size());
//     }

//     // 内部类：表示边的坐标
//     private static class EdgeCoordinates {
//         double startX, startY, endX, endY;

//         EdgeCoordinates(double startX, double startY, double endX, double endY) {
//             this.startX = startX;
//             this.startY = startY;
//             this.endX = endX;
//             this.endY = endY;
//         }
//     }

//     @Override
//     public void cleanup() {
//         if (db != null) {
//             db.close();
//         }
//     }
// }