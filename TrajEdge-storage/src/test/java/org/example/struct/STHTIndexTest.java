// package org.example.struct;

// import org.example.trajstore.TrajPoint;
// import org.junit.Before;
// import org.junit.Test;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;

// import java.util.ArrayList;
// import java.util.List;
// import static org.junit.Assert.*;

// public class STHTIndexTest {
//     private static final Logger LOG = LoggerFactory.getLogger(STHTIndexTest.class);
//     private STHTIndex index;

//     @Before
//     public void setUp() {
//         index = new STHTIndex();
//     }

//     @Test
//     public void testEncodeTime() {
//         // 测试时间编码
//         long startTime = 1609459200L + 60 * 60 * 8; // 2021-01-01 00:00:00
//         long endTime =   startTime + 60 * 60 * 8;   // 8小时后
//         String[] timeCode = index.encodeTime(startTime, endTime);
        
//         LOG.info("binNum: " + timeCode[0]);
//         LOG.info("tCode: " + timeCode[1]);
//     }

//     @Test
//     public void testEncodeSpatial() {
//         // 测试空间编码
//         double minLat = 39.9, maxLat = 40.1;
//         double minLng = 116.3, maxLng = 116.5;

//         String spatialCode = index.encodeSpatial(minLat, maxLat, minLng, maxLng);
        
//         LOG.info(spatialCode);
//         // 验证空间编码的格式和长度
//         // assertTrue(spatialCode.matches("[01]+"));
//         // assertTrue(spatialCode.length() > 4); // 至少包含 posCode (4位) 和一些 xz2 编码
//     }

//     @Test
//     public void testInsertTrajectory() {
//         // 创建一个简单的轨迹
//         List<TrajPoint> trajectory = new ArrayList<>();
//         trajectory.add(new TrajPoint(1, 1609459200L, 0L, 0.0, 39.9, 116.3));
//         trajectory.add(new TrajPoint(1, 1609459260L, 0L, 0.0, 39.95, 116.35));
//         trajectory.add(new TrajPoint(1, 1609459320L, 0L, 0.0, 40.0, 116.4));

//         List<TrajPoint> trajectory2 = new ArrayList<>();
//         trajectory2.add(new TrajPoint(2, 1709459400L, 0L, 0.0, 39.9125, 116.391));
//         trajectory2.add(new TrajPoint(2, 1709459460L, 0L, 0.0, 39.81, 116.49));
//         trajectory2.add(new TrajPoint(2, 1709459620L, 0L, 0.0, 40.0, 117.0));

//         // 插入轨迹
//         index.insertTrajectory(trajectory);
//         index.insertTrajectory(trajectory2);

//         // 查询轨迹
//         List<Integer> result = index.query(1609459199L, 1609459321L, 39.8, 40.1, 116.2, 116.5);

//         // 验证查询结果
//         assertFalse(result.isEmpty());
//         assertTrue(result.contains(1));
//         assertFalse(result.contains(2));
//     }

//     @Test
//     public void testQueryNonExistentTrajectory() {
//         // 查询一个不存在的轨迹
//         List<Integer> result = index.query(1609459200000L, 1609459320000L, 0, 1, 0, 1);

//         // 验证查询结果为空
//         assertTrue(result.isEmpty());
//     }

//     @Test
//     public void testGetPredecessorIds() {
//         // 插入一些轨迹数据
//         insertSampleTrajectories();

//         // 测试获取前驱节点
//         List<STHTIndex.TrieNode> predecessors = index.getPredecessorIds("1010101");

//         // 验证前驱节点
//         assertFalse(predecessors.isEmpty());
//         assertTrue(predecessors.size() <= 2);
//         for (STHTIndex.TrieNode node : predecessors) {
//             assertTrue(node.prefix.compareTo("1010101") < 0);
//         }
//     }

//     @Test
//     public void testGetSuccessorIds() {
//         // 插入一些轨迹数据
//         insertSampleTrajectories();

//         // 测试获取后继节点
//         List<STHTIndex.TrieNode> successors = index.getSuccessorIds("1010101");

//         // 验证后继节点
//         assertFalse(successors.isEmpty());
//         assertTrue(successors.size() <= 2);
//         for (STHTIndex.TrieNode node : successors) {
//             assertTrue(node.prefix.compareTo("1010101") > 0);
//         }
//     }

//     private void insertSampleTrajectories() {
//         // 插入一些样本轨迹数据以测试前驱和后继
//         for (int i = 0; i < 10; i++) {
//             List<TrajPoint> trajectory = new ArrayList<>();
//             long startTime = 1609459200000L + i * 3600000;
//             trajectory.add(new TrajPoint(i, startTime, 0L, 0.0, 39.9 + i * 0.01, 116.3 + i * 0.01));
//             trajectory.add(new TrajPoint(i, startTime + 300000, 0L, 0.0, 39.91 + i * 0.01, 116.31 + i * 0.01));
//             trajectory.add(new TrajPoint(i, startTime + 600000, 0L, 0.0, 39.92 + i * 0.01, 116.32 + i * 0.01));
//             index.insertTrajectory(trajectory);
//         }
//     }
// }