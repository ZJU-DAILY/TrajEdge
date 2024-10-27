/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.example.spout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RandomPointSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(RandomPointSpout.class);
    private SpoutOutputCollector collector;
    private Integer pointer = 0;
    private String[] list;
    private List<Values> values;
    private Long maxTrajectoryIndex = 1000L;

    private String dataSrc;
    private Integer lastTrajId = -1;
    private int sentTuples = 0;
    private int totalTuples;
    private boolean finished = false;

    private static final int LOG_INTERVAL = 100; // 每发送1000个tuple记录一次日志

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        maxTrajectoryIndex = Long.parseLong((String) conf.get("trajNum"));
        LOG.info("trajNum: {}", maxTrajectoryIndex);
        this.dataSrc = (String) conf.get("data.src");
        this.collector = collector;
        File path = new File(this.dataSrc);
        list = path.list();
        values = new ArrayList<>();
        readFromFile();
        totalTuples = values.size();
        LOG.info("read file done, total tuples: " + totalTuples);
    }

    @Override
    public void nextTuple() {
        if (finished) {
            Utils.sleep(100);
            return;
        }

        if (sentTuples < values.size()) {
            collector.emit(values.get(pointer), pointer);
            sentTuples++;
            
            // 每发送LOG_INTERVAL个tuple，记录一次积压情况
            if (sentTuples % LOG_INTERVAL == 0) {
                int pendingTuples = totalTuples - sentTuples;
                LOG.info("RandomPointSpout - Pending tuples: " + pendingTuples + 
                         ", Sent tuples: " + sentTuples + 
                         ", Total tuples: " + totalTuples);
            }

            if(!values.get(pointer).get(0).equals(lastTrajId)){
                lastTrajId = (Integer) values.get(pointer).get(0);
                LOG.info("Start upload of trajectory: " + lastTrajId);
                LOG.info("maxTrajectoryIndex: " + maxTrajectoryIndex);
            }
            pointer++;
        } else {
            // 所有tuple都已发送，发出结束信号
            // collector.emit(new Values(-1, -1L, -1L, -0.0, -0.0, -0.0), "END");
            finished = true;
            LOG.info("All tuples have been processed. Sent end-of-stream signal.");
        }
    }

    public void readFromFile() {
        try {
            for (String file : list) {
               LOG.info("Trajectory id: {}", file);
                if(values.size() > this.maxTrajectoryIndex)break;
                BufferedReader in = new BufferedReader(new FileReader(this.dataSrc + file));
                String str;
                while ((str = in.readLine()) != null) {
                    String[] points = str.split("\n");
                    for (String point : points) {
                        int trajId = Integer.parseInt(file);
                        String[] info = point.split(" ");
                        double lat = Double.parseDouble(info[0]);
                        double lng = Double.parseDouble(info[1]);
                        long timestamp = Long.parseLong(info[2]);
                        values.add(new Values(trajId, timestamp, 0L, 0.0, lat, lng));
                        timestamp++;
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void ack(Object msgId) {
        super.ack(msgId);
    }

    @Override
    public void fail(Object msgId) {
        collector.emit(values.get((Integer) msgId), msgId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("trajId", "timestamp", "edgeId", "dist", "oriLat", "oriLng"));
    }

}
