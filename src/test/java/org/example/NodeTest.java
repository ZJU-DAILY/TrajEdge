package org.example;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.example.grpc.TrajectoryServiceGrpc;
import org.example.struct.STHTIndex;
import org.example.grpc.TrajectoryRequest;
import org.example.grpc.TrajectoryResponse;
import org.example.grpc.TrajectoryPoint;
import org.example.trajstore.TrajPoint;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import static org.junit.Assert.*;
import java.util.HashSet;
import java.util.Set;


import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class NodeTest {
    private static final Logger LOG = LoggerFactory.getLogger(NodeTest.class);
    private static final int NODE1_PORT = 50051;
    private static final int NODE2_PORT = 50052;
    private static TrajectoryServiceGrpc.TrajectoryServiceBlockingStub stub1;
    private static TrajectoryServiceGrpc.TrajectoryServiceBlockingStub stub2;

    @Before
    public void setup() throws Exception {
        // Start Node1
        ExecutorService executor = Executors.newFixedThreadPool(2);
        executor.submit(() -> {
            try {
                NodeLauncher.main(new String[]{String.valueOf(NODE1_PORT)});
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // Start Node2
        executor.submit(() -> {
            try {
                NodeLauncher.main(new String[]{String.valueOf(NODE2_PORT)});
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // Wait for nodes to start
        TimeUnit.SECONDS.sleep(10);

        // Create gRPC stubs for communication
        ManagedChannel channel1 = ManagedChannelBuilder.forAddress("localhost", NODE1_PORT)
                .usePlaintext()
                .build();
        ManagedChannel channel2 = ManagedChannelBuilder.forAddress("localhost", NODE2_PORT)
                .usePlaintext()
                .build();

        stub1 = TrajectoryServiceGrpc.newBlockingStub(channel1);
        stub2 = TrajectoryServiceGrpc.newBlockingStub(channel2);
    }

    @Test
    public void testNodeDataInsertion() {
        // Insert data into Node1 until it's full
        String result = "";
        long stepTime = 12 * 60 * 60 * 1000;
        int i = 0;
        while(result == ""){
            List<TrajectoryPoint> trajectory = createTestTrajectory(i + 1, 1000000000L + i * stepTime, 1000086400L + i * stepTime, 30.0, 31.0, 120.0, 121.0);
            result = insertTrajectory(stub1, trajectory);
            i += 1;
        }
        
        LOG.info("Insert to sub1 finished: " + result);

        // Verify that Node1 is full and returns the next node ID
        assertNotNull(result);
        assertEquals(String.valueOf(NODE1_PORT + 1), result);

        List<TrajectoryPoint> trajectory2 = createTestTrajectory(i + 1, 1000000000L + i * stepTime, 1000086400L + i * stepTime, 31.0, 32.0, 121.0, 122.0);
        // Insert data into Node2
        result = insertTrajectory(stub2, trajectory2);
        assertTrue(result == "");

        // Read data from Node1
        List<TrajectoryPoint> readData1 = readTrajectoryData(stub1, 1000000000L, 1000086400L + (i + 1) * stepTime, 30.0, 31.0, 120.0, 121.0);
        Set<Integer> trajIds = new HashSet<>();

        for(TrajectoryPoint point : readData1){
            trajIds.add(point.getTrajId());
        }
        assertTrue(trajIds.contains(i + 1));

        // Read data from Node2
        // List<TrajectoryPoint> readData2 = readTrajectoryData(stub2, 1000000000L + i * stepTime, 1000086400L + i * stepTime, 31.0, 32.0, 121.0, 122.0);
        // assertFalse(readData2.isEmpty());
    }

    private String insertTrajectory(TrajectoryServiceGrpc.TrajectoryServiceBlockingStub stub, List<TrajectoryPoint> trajectory) {
        TrajectoryRequest request = TrajectoryRequest.newBuilder()
                .addAllPoints(trajectory)
                .build();
        TrajectoryResponse response = stub.addTrajectoryData(request);
        return response.getNextNodeId();
    }

    private List<TrajectoryPoint> readTrajectoryData(TrajectoryServiceGrpc.TrajectoryServiceBlockingStub stub,
                                                     long startTime, long endTime, double minLat, double maxLat,
                                                     double minLng, double maxLng) {
        TrajectoryRequest request = TrajectoryRequest.newBuilder()
                .setStartTime(startTime)
                .setEndTime(endTime)
                .setMinLat(minLat)
                .setMaxLat(maxLat)
                .setMinLng(minLng)
                .setMaxLng(maxLng)
                .build();
        TrajectoryResponse response = stub.readTrajectoryData(request);
        return response.getPointsList();
    }

    private List<TrajectoryPoint> createTestTrajectory(int trajId, long startTime, long endTime,
                                                       double minLat, double maxLat, double minLng, double maxLng) {
        List<TrajectoryPoint> trajectory = new ArrayList<>();
        long timeStep = (endTime - startTime) / 10;
        double latStep = (maxLat - minLat) / 10;
        double lngStep = (maxLng - minLng) / 10;

        for (int i = 0; i < 10; i++) {
            trajectory.add(TrajectoryPoint.newBuilder()
                    .setTrajId(trajId)
                    .setTimestamp(startTime + i * timeStep)
                    .setEdgeId(0)
                    .setDistance(0.0)
                    .setLat(minLat + i * latStep)
                    .setLng(minLng + i * lngStep)
                    .build());
        }

        return trajectory;
    }
}
