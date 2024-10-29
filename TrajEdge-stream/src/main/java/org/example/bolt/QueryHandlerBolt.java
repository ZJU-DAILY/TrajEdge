package org.example.bolt;

import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.DaemonConfig;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.example.grpc.TrajectoryPoint;
import org.example.grpc.TrajectoryRequest;
import org.example.grpc.TrajectoryResponse;
import org.example.grpc.TrajectoryServiceGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * @author alecHe
 * @desc ...
 * @date 2023-11-22 15:59:05
 */
public class QueryHandlerBolt extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(QueryHandlerBolt.class);
    private Map<String, Object> stormConf;
    private TopologyContext context;
    private TrajectoryServiceGrpc.TrajectoryServiceBlockingStub stub;
    private Integer port;


    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.stormConf = stormConf;
        this.context = context;

        port = 9999;
        ManagedChannel channel1 = ManagedChannelBuilder.forAddress("localhost", port)
            .usePlaintext()
            .build();
        stub = TrajectoryServiceGrpc.newBlockingStub(channel1);
    }

  

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Long startTime = input.getLongByField("startTime");
        Long endTime = input.getLongByField("endTime");
        Double minLat = input.getDoubleByField("minLat");
        Double maxLat = input.getDoubleByField("maxLat");
        Double minLng = input.getDoubleByField("minLng");
        Double maxLng = input.getDoubleByField("maxLng");
       
        TrajectoryRequest request = TrajectoryRequest.newBuilder()
                .setStartTime(startTime)
                .setEndTime(endTime)
                .setMinLat(minLat)
                .setMaxLat(maxLat)
                .setMinLng(minLng)
                .setMaxLng(maxLng)
                .build();
        TrajectoryResponse response = stub.readTrajectoryData(request);
        List<TrajectoryPoint> readData = response.getPointsList();

        LOG.info("point count number {}.", readData.size());

        StringBuffer sb = new StringBuffer();
        for(TrajectoryPoint point : readData){
            sb.append(point.getTrajId());
            sb.append(", ");
        }

        LOG.info("all traj point id list: {}.", sb.toString());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }

    @Override
    public void cleanup() {
    }
}
