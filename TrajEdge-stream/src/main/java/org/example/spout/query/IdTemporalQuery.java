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
public class IdTemporalQuery extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(IdTemporalQuery.class);
    private static final Integer repeat = 1000;
    private SpoutOutputCollector collector;
    private Random random;
    private List<Long> temporalRange;
    private Integer counter = 0;
    private int k, id;
    private double minLat, maxLat, minLng, maxLng;
    private long startTime, endTime;


    public IdTemporalQuery(int k, int id, double minLat, double maxLat, double minLng, double maxLng, long startTime, long endTime){
        this.random = new Random(10086);
        this.k = k;
        this.startTime = startTime;
        this.endTime = endTime;
        this.id = id;

        this.minLat = minLat;
        this.maxLat = maxLat;
        this.minLng = minLng;
        this.maxLng = maxLng;
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    /**
     * 将时间范围等分并随机选择一个时间区域
     * @param k 分数
     * @param startTime 开始时间
     * @param endTime 结束时间
     * @return 随机选择的时间区域
     */
    public List<Long> divideAndSelectTimeRange(int k, long startTime, long endTime) {
        long timeStep = (endTime - startTime) / k;

        int randomTimeIndex = random.nextInt(k);

        long selectedStartTime = startTime + randomTimeIndex * timeStep;
        long selectedEndTime = selectedStartTime + timeStep;

        List<Long> selectedTimeRange = new ArrayList<>();
        selectedTimeRange.add(selectedStartTime);
        selectedTimeRange.add(selectedEndTime);

        return selectedTimeRange;
    }

    @Override
    public void nextTuple() {
        if(this.counter > repeat) Utils.sleep(100);
        else{
            this.temporalRange = divideAndSelectTimeRange(k, startTime, endTime); // 初始化 temporalRange
            this.counter++;
            collector.emit(new Values(1, id, -1, temporalRange.get(0), temporalRange.get(1), minLat, maxLat, minLng, maxLng));
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
        declarer.declare(new Fields("queryType", "trajId", "topk", "startTime", "endTime", "minLat", "maxLat", "minLng", "maxLng"));
    }
}
