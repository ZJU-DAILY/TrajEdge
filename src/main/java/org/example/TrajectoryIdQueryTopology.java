/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.example.bolt.QueryHandlerBolt;
import org.example.bolt.QueryHandlerofDHTBolt;
import org.example.spout.query.IdQuerySpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;


public class TrajectoryIdQueryTopology {

    public static void main(String[] args) throws Exception {

        boolean isCluster = false;
        String topoName = "trajectoryIdQuery";
        int trajId = 1;
        long startTime = -1L;
        long endTime = -1L;
        if (args.length > 0) {
            topoName = args[0];
            if (args.length > 1) {
                isCluster = true;
            }
            if (args.length > 2) {
                trajId = Integer.parseInt(args[2]);
            }
            if(args.length == 5){
                startTime = Long.parseLong(args[3]);
                endTime = Long.parseLong(args[4]);
            }
        }

        Config config = new Config();
        config.put("trajId", trajId);
        config.put("startTime", startTime);
        config.put("endTime", endTime);
        config.setNumWorkers(1);
        if (!isCluster) {
            config.setDebug(false);
            config.put("data.src", "F:/File-D/mapOut/");
            config.put("data.index.dest", "F:/IndexStoreTest/");
            config.put("data.dest", "F:/RocksDbStoreTest/");
        }

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new IdQuerySpout(), 1);
        builder.setBolt("idQuery", new QueryHandlerBolt(), 1).fieldsGrouping("spout", new Fields("trajId"));


        if (!isCluster) {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology(topoName, config, builder.createTopology());
        } else {
            StormSubmitter.submitTopologyWithProgressBar(topoName, config, builder.createTopology());
        }


    }

}
