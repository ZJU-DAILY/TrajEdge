package org.example.spout.query;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
//import org.agrona.collections.LongArrayList;
import java.util.Random;
import java.util.Set;
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
    private Integer queryNum = 0;
    private List<Long> mockRange;
    private int pointer = 0;
//    private int all = 165279;
    private int all = 10000;

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        queryNum = (Integer) conf.get("ratio") * all / 100;
        Set<Long> uniqueRandomNumbers = generateUniqueRandomNumbers(0L, all, queryNum);
        mockRange = new ArrayList<>(uniqueRandomNumbers);
    }

    private static Set<Long> generateUniqueRandomNumbers(long min, long max, int count) {
        if (count > (max - min + 1) || max < min) {
            throw new IllegalArgumentException("Invalid range or count");
        }
        long seed = 2;
        Set<Long> uniqueNumbers = new HashSet<>();
        Random random = new Random(seed);

        while (uniqueNumbers.size() < count) {
            long randomNumber = ((long) (random.nextDouble() * (max - min + 1))) + min;
            uniqueNumbers.add(randomNumber);
        }

        return uniqueNumbers;
    }

    @Override
    public void nextTuple() {
        Utils.sleep(100);
        if (pointer < queryNum) {
            collector.emit(new Values(1, mockRange.get(pointer),-1L, -1L));
            pointer++;
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
