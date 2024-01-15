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

package org.example.spout.query;

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

public class IdQuerySpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(IdQuerySpout.class);
    private SpoutOutputCollector collector;
    private boolean first = true;
    private int trajId;
    private long startTime;
    private long endTime;

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        trajId = (Integer) conf.get("trajId");
        startTime = ((Integer) conf.get("startTime")).longValue();
        endTime = ((Integer) conf.get("endTime")).longValue();
    }

    @Override
    public void nextTuple() {
        Utils.sleep(100);

        if (first) {
            first = false;
            collector.emit(new Values(trajId, startTime, endTime));
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
        declarer.declare(new Fields("trajId", "startTime", "endTime"));
    }

}
