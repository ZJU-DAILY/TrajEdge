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

public class RandomTrajectorySpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(RandomTrajectorySpout.class);
    private SpoutOutputCollector collector;
    private Integer pointer = 0;
    private Integer m = 1;
    private Integer trajIdPointer = 0;
    private String[] list;
    private List<Values> values;
    private Integer maxTrajectoryIndex = 1000;

    private String dataSrc;

    private List<Integer> msgIds;
    private Integer lastTrajId = -1;

    private static Date seconds(int seconds) {
        Calendar c = new GregorianCalendar(2014, 1, 1);
        c.add(Calendar.SECOND, seconds);
        return c.getTime();
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        maxTrajectoryIndex = (Integer) conf.get("trajNum");
        this.dataSrc = (String) conf.get("data.src");
        this.collector = collector;
        File path = new File(this.dataSrc);
        list = path.list();
        values = new ArrayList<>();
        readFromFile();
        LOG.info("read file done");
    }

    @Override
    public void nextTuple() {
        Utils.sleep(100);
        if(maxTrajectoryIndex < 0)return;
        collector.emit(values.get(pointer), pointer);
        if(values.get(pointer).get(0) != lastTrajId){
            maxTrajectoryIndex--;
            lastTrajId = (Integer) values.get(pointer).get(0);
            LOG.info("Start upload of trajectory: " + lastTrajId);
        }
        pointer++;
    }

    public void readFromFile() {
        try {
            for (String file : list) {
//                LOG.info("Trajectory id: {}", file);
                BufferedReader in = new BufferedReader(new FileReader(this.dataSrc + file));
                String str;
                while ((str = in.readLine()) != null) {
                    String[] edges = str.split(",");
                    Long timestamp = 0L;
                    for (String edgeStr : edges) {
                        int trajId = Integer.parseInt(file);
                        long edge = Long.parseLong(edgeStr);
                        values.add(new Values(trajId, timestamp, edge, 0.0));
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
//        declarer.declare(new Fields("trajId", "timestamp", "lat", "lng"));
        declarer.declare(new Fields("trajId", "timestamp", "edgeId", "dist"));
    }

}
