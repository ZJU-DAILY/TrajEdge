package org.example.spout.query;

import java.util.List;
import java.util.Map;
import org.agrona.collections.LongArrayList;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author alecHe
 * @desc ...
 * @date 2023-11-22 15:44:05
 */
public class SpacialRangeQuerySpout extends BaseRichSpout {

    private static final Logger LOG = LoggerFactory.getLogger(SpacialRangeQuerySpout.class);
    SpoutOutputCollector collector;
    boolean first = true;

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        Utils.sleep(100);
        List<Long> mockRange = new LongArrayList();
        mockRange.add(9810L);
        mockRange.add(9809L);
        mockRange.add(9808L);
        mockRange.add(9807L);

        if (first) {
            first = false;
            collector.emit(new Values(1, mockRange, 50L, 60L));
        }


    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("queryId", "SpacialRange", "startTime", "endTime"));
    }
}
