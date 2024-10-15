package org.example.bolt;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SthtIndexBolt extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SthtIndexBolt.class);
    private static final long REF_TIME = 0; // 1970-01-01T00:00:00Z
    private static final int BIN_LEN = 86400; // 一天的秒数

    private static final int BETA = 2; // 分区大小

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
       
    }


    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
       
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public void cleanup() {
        
    }

}
