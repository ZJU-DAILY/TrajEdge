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
public class SpacialTemporalRangeQuerySpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(SpacialRangeQuerySpout.class);
    private static final Integer repeat = 100;
    private SpoutOutputCollector collector;
    private List<Double> spacialRange;
    private Random random;
    private List<Long> temporalRange;
    private Integer counter = 0;
    private int k;
    private double minLat, maxLat, minLng, maxLng;
    private long startTime, endTime;


    public SpacialTemporalRangeQuerySpout(int k, double minLat, double maxLat, double minLng, double maxLng, long startTime, long endTime){
        this.random = new Random(10086);
        this.k = k;
        this.minLat = minLat;
        this.maxLat = maxLat;
        this.minLng = minLng;
        this.maxLng = maxLng;
        this.startTime = startTime;
        this.endTime = endTime;
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

        /**
     * 将区域等分并随机选择一个区域
     * @param k 分数
     * @param maxLat 最大纬度
     * @param minLat 最小纬度
     * @param maxLng 最大经度
     * @param minLng 最小经度
     * @return 随机选择的区域
     */
    public List<Double> divideAndSelectRegion(int k, double maxLat, double minLat, double maxLng, double minLng) {
        double latStep = (maxLat - minLat) / k;
        double lngStep = (maxLng - minLng) / k;

        int randomLatIndex = random.nextInt(k);
        int randomLngIndex = random.nextInt(k);

        double selectedMinLat = minLat + randomLatIndex * latStep;
        double selectedMaxLat = selectedMinLat + latStep;
        double selectedMinLng = minLng + randomLngIndex * lngStep;
        double selectedMaxLng = selectedMinLng + lngStep;

        List<Double> selectedRegion = new ArrayList<>();
        selectedRegion.add(selectedMinLat);
        selectedRegion.add(selectedMaxLat);
        selectedRegion.add(selectedMinLng);
        selectedRegion.add(selectedMaxLng);

        return selectedRegion;
    }

    @Override
    public void nextTuple() {
        if(this.counter >= repeat) Utils.sleep(100);
        else{
            this.spacialRange = divideAndSelectRegion(k, maxLat, minLat, maxLng, minLng);
            this.temporalRange = divideAndSelectTimeRange(k, startTime, endTime); // 初始化 temporalRange
            this.counter++;
            collector.emit(new Values(3, -1, -1, temporalRange.get(0), temporalRange.get(1), spacialRange.get(0), 
                spacialRange.get(1), spacialRange.get(2), spacialRange.get(3)));
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
