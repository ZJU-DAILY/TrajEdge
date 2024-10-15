// package org.example.struct;

// import org.example.trajstore.TrajPoint;
// import org.junit.Before;
// import org.junit.Test;
// import java.util.ArrayList;
// import java.util.List;
// import static org.junit.Assert.*;

// public class NodeTest {
//     private Node node;
//     private List<TrajPoint> trajectory1;
//     private List<TrajPoint> trajectory2;

//     @Before
//     public void setUp() {
//         node = new Node("testNode", "0");
//         trajectory1 = createTestTrajectory(1, 1000000000L, 1000086400L, 30.0, 31.0, 120.0, 121.0);
//         trajectory2 = createTestTrajectory(2, 1000172800L, 1000259200L, 31.0, 32.0, 121.0, 122.0);
//     }

//     @Test
//     public void testAddData() {
//         node.addData(trajectory1);
//         node.addData(trajectory2);
        
//         // TODO: Add assertions to verify that data was added correctly
//         // This might involve checking the TrajStore or the STHTIndex
//     }

//     @Test
//     public void testSplitNode() {
//         // Add enough data to trigger a split
//         for (int i = 0; i < 1000; i++) {
//             node.addData(createTestTrajectory(i, 1000000000L + i * 86400L, 1000086400L + i * 86400L, 
//                                               30.0 + i * 0.1, 31.0 + i * 0.1, 120.0 + i * 0.1, 121.0 + i * 0.1));
//         }
        
//         // TODO: Add assertions to verify that the node was split correctly
//         // This might involve checking the RoutingTable and the new Node created
//     }

//     private List<TrajPoint> createTestTrajectory(int trajId, long startTime, long endTime, 
//                                                  double minLat, double maxLat, double minLng, double maxLng) {
//         List<TrajPoint> trajectory = new ArrayList<>();
//         long timeStep = (endTime - startTime) / 10;
//         double latStep = (maxLat - minLat) / 10;
//         double lngStep = (maxLng - minLng) / 10;

//         for (int i = 0; i < 10; i++) {
//             trajectory.add(new TrajPoint(trajId, startTime + i * timeStep, 0L, 0.0, 
//             minLat + i * latStep, minLng + i * lngStep));
//         }

//         return trajectory;
//     }
// }