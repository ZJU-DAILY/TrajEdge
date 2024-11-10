package org.example.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.example.grpc.TrajectoryRequest;
import org.example.grpc.TrajectoryResponse;
import org.example.grpc.TrajectoryPoint;
import org.example.grpc.TrajectoryServiceGrpc;

public class DataStoreBolt extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(DataStoreBolt.class);
    private Map<String, Object> stormConf;
    private TopologyContext context;
    private TrajectoryServiceGrpc.TrajectoryServiceBlockingStub stub;
    private Integer port;
    private Map<Integer, List<TrajectoryPoint>> trajectoryBuffer;
    private int totalPoints;
    private static final Integer BUFFER_SIZE = 10000;
    private List<ManagedChannel> channels = new ArrayList<>();

    
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.stormConf = stormConf;
        this.context = context;
        port = 9999;
        ManagedChannel channel1 = ManagedChannelBuilder.forAddress("localhost", port)
            .usePlaintext()
            .build();
        stub = TrajectoryServiceGrpc.newBlockingStub(channel1);
        trajectoryBuffer = new HashMap<>();
        totalPoints = 0;
        LOG.info("data store is prepared...");
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Integer trajId = input.getIntegerByField("trajId");
        Long timestamp = input.getLongByField("timestamp");
        Long edgeId = input.getLongByField("edgeId");
        Double dist = input.getDoubleByField("dist");
        Double oriLng = input.getDoubleByField("oriLng"), oriLat = input.getDoubleByField("oriLat");

        TrajectoryPoint point = TrajectoryPoint.newBuilder()
            .setTrajId(trajId)
            .setTimestamp(timestamp)
            .setEdgeId(edgeId)
            .setDistance(dist)
            .setLat(oriLat)
            .setLng(oriLng)
            .build();

        trajectoryBuffer.computeIfAbsent(trajId, k -> new ArrayList<>()).add(point);
        totalPoints++;

        if (totalPoints >= BUFFER_SIZE) {
            flushBuffer();
        }
    }

    private void flushBuffer() {
        if (trajectoryBuffer.isEmpty()) {
            return;
        }

        TrajectoryServiceGrpc.TrajectoryServiceBlockingStub localStub = this.stub;
        
        for (List<TrajectoryPoint> trajectory : trajectoryBuffer.values()) {
            String result = "";
            while (true) {
                result = insertTrajectory(localStub, trajectory);
                if (result.isEmpty()) {
                    break;
                } else {
                    ManagedChannel channel = ManagedChannelBuilder.forAddress(result, port)
                        .usePlaintext()
                        .build();
                    channels.add(channel);
                    localStub = TrajectoryServiceGrpc.newBlockingStub(channel);
                }
            }
        }

        trajectoryBuffer.clear();
        totalPoints = 0;
        LOG.info("Flushed {} trajectories to storage", trajectoryBuffer.size());
    }

    private String insertTrajectory(TrajectoryServiceGrpc.TrajectoryServiceBlockingStub stub, List<TrajectoryPoint> trajectory) {
        TrajectoryRequest request = TrajectoryRequest.newBuilder()
                .addAllPoints(trajectory)
                .build();
        TrajectoryResponse response = stub.addTrajectoryData(request);
        return response.getNextNodeId();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }

    @Override
    public void cleanup() {
        flushBuffer();
        for (ManagedChannel channel : channels) {
            channel.shutdown();
        }
    }
}
