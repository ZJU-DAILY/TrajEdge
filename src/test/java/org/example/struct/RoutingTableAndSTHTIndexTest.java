// package org.example.struct;

// import org.example.trajstore.TrajPoint;
// import org.junit.Before;
// import org.junit.Test;
// import java.util.ArrayList;
// import java.util.List;
// import static org.junit.Assert.*;

// public class RoutingTableAndSTHTIndexTest {
//     private RoutingTable routingTable;
//     private STHTIndex sthtIndex;

//     @Before
//     public void setUp() {
//         routingTable = new RoutingTable();
//         sthtIndex = new STHTIndex();
//     }

//     @Test
//     public void testRoutingTable() {
//         routingTable.addPredecessors(List.of("00", "01"), "node1");
//         routingTable.addSuccessors(List.of("10", "11"), "node2");

//         assertEquals("node1", routingTable.getPredecessor("00"));
//         assertEquals("node1", routingTable.getPredecessor("01"));
//         assertEquals("node2", routingTable.getSuccessor("10"));
//         assertEquals("node2", routingTable.getSuccessor("11"));
//         assertNull(routingTable.getPredecessor("10"));
//         assertNull(routingTable.getSuccessor("00"));
//     }

//     @Test
//     public void testSTHTIndex() {
//         List<TrajPoint> trajectory = createTestTrajectory(1, 1000000000L, 1000086400L, 30.0, 31.0, 120.0, 121.0);
//         sthtIndex.insertTrajectory(trajectory);

//         List<Integer> queryResult = sthtIndex.query(1000000000L, 1000086400L, 29.5, 31.5, 119.5, 121.5);
//         assertTrue(queryResult.contains(1));

//         queryResult = sthtIndex.query(1100000000L, 1100086400L, 29.5, 31.5, 119.5, 121.5);
//         assertFalse(queryResult.contains(1));
//     }

//     @Test
//     public void testSTHTIndexPredecessorsAndSuccessors() {
//         // Insert some test data
//         for (int i = 0; i < 10; i++) {
//             List<TrajPoint> trajectory = createTestTrajectory(i, 1000000000L + i * 86400L, 1000086400L + i * 86400L, 
//                                                               30.0 + i * 0.1, 31.0 + i * 0.1, 120.0 + i * 0.1, 121.0 + i * 0.1);
//             sthtIndex.insertTrajectory(trajectory);
//         }

//         // Test getPredecessorIds
//         List<STHTIndex.TrieNode> predecessors = sthtIndex.getPredecessorIds("1010");
//         assertFalse(predecessors.isEmpty());
//         // Add more specific assertions based on your expected behavior

//         // Test getSuccessorIds
//         List<STHTIndex.TrieNode> successors = sthtIndex.getSuccessorIds("1010");
//         assertFalse(successors.isEmpty());
//         // Add more specific assertions based on your expected behavior
//     }

//     private List<TrajPoint> createTestTrajectory(int trajId, long startTime, long endTime, 
//                                                  double minLat, double maxLat, double minLng, double maxLng) {
//         List<TrajPoint> trajectory = new ArrayList<>();
//         long timeStep = (endTime - startTime) / 10;
//         double latStep = (maxLat - minLat) / 10;
//         double lngStep = (maxLng - minLng) / 10;

//         for (int i = 0; i < 10; i++) {
//             trajectory.add(new TrajPoint(trajId, startTime + i * timeStep, 0L,0.0,
//                                          minLat + i * latStep, minLng + i * lngStep));
//         }

//         return trajectory;
//     }
// }