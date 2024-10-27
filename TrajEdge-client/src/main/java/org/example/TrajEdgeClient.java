package org.example;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.example.grpc.TrajectoryServiceGrpc;
import org.example.grpc.TrajectoryRequest;
import org.example.grpc.TrajectoryResponse;
import org.example.grpc.TrajectoryPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import java.util.TreeSet;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TrajEdgeClient {
    private static final Logger LOG = LoggerFactory.getLogger(TrajEdgeClient.class);
    
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: NodeLauncher <port>");
            System.exit(1);
        }

        int port = Integer.parseInt(args[0]);
        ManagedChannel channel1 = ManagedChannelBuilder.forAddress("localhost", port)
            .usePlaintext()
            .build();

        TrajectoryServiceGrpc.TrajectoryServiceBlockingStub stub1 = TrajectoryServiceGrpc.newBlockingStub(channel1);

        // // Insert data into Node1 until it's full
        // String result = "";
        // long stepTime = 24 * 60 * 60 * 1000;
        // int i = 0;
        // while(result == ""){
        //     List<TrajectoryPoint> trajectory = createTestTrajectory(i + 1, 1609459200L + i * stepTime, 1609559200L + i * stepTime, 30.0, 31.0, 120.0, 121.0);
        //     result = insertTrajectory(stub1, trajectory);
        //     i += 1;
        //     // break;
        // }
        
        // LOG.info("Insert to sub1 finished: " + result);

        // List<TrajectoryPoint> trajectory2 = createTestTrajectory(i + 1, 1609459200L + i * stepTime, 1609559200L + i * stepTime, 31.0, 32.0, 121.0, 122.0);

        // ManagedChannel channel2 = ManagedChannelBuilder.forAddress(result, port)
        //     .usePlaintext()
        //     .build();

        // TrajectoryServiceGrpc.TrajectoryServiceBlockingStub stub2 = TrajectoryServiceGrpc.newBlockingStub(channel2);

        // // Insert data into Node2
        // result = insertTrajectory(stub2, trajectory2);
        
        // LOG.info("Insert to sub2 finished: " + result);

        // Read data from Node1
        // List<TrajectoryPoint> readData1 = readTrajectoryData(stub1, 1609459000L, 1609559300L + (i + 1) * stepTime, 30.0, 31.0, 120.0, 121.0);
        List<TrajectoryPoint> readData1 = readTrajectoryData(stub1, 1211507919L, 1211520582L, 39.0, 40.0, 116.0, 117.0);

        for(TrajectoryPoint point : readData1){
            LOG.info(point.getTrajId() + " " + point.getTimestamp() + " " +point.getEdgeId() 
            + " " + point.getDistance() + " " + point.getLat() + " " + point.getLng());
        }
   
    }

    private static String insertTrajectory(TrajectoryServiceGrpc.TrajectoryServiceBlockingStub stub, List<TrajectoryPoint> trajectory) {
        TrajectoryRequest request = TrajectoryRequest.newBuilder()
                .addAllPoints(trajectory)
                .build();
        TrajectoryResponse response = stub.addTrajectoryData(request);
        return response.getNextNodeId();
    }

    private static List<TrajectoryPoint> readTrajectoryData(TrajectoryServiceGrpc.TrajectoryServiceBlockingStub stub,
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

    private static List<TrajectoryPoint> createTestTrajectory(int trajId, long startTime, long endTime,
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
