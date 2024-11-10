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
import org.example.spout.query.IdTemporalQuery;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;


public class TrajectoryIdQueryTopology {

    public static void main(String[] args) throws Exception {
        boolean isCluster = false;
        String number = "1";
        int trajId = 1;
        int k = 8;
        String dataset = "geolife";
        double minLat, maxLat, minLng, maxLng;
        long startTime = 1176341492L, endTime = 1343349080L;
        minLat = 1.044024;
        maxLat = 63.0141583;
        minLng = -179.9695933;
        maxLng = 179.9969416;

        if (args.length > 0) {
            isCluster = true;
            number = args[0];
        }
        if (args.length > 1) {
            trajId = Integer.parseInt(args[1]);
        }
        if(args.length > 2){
            k = Integer.parseInt(args[2]);
        }
        if (args.length > 3) {
            dataset = args[3];
        }

        Config config = new Config();
        config.setNumWorkers(1);

        if("tdrive".equals(dataset)){
            minLat = 0.0;
            maxLat = 65.20465;
            minLng = 0.0;
            maxLng = 174.06752;
        }

        String topoName = "TrajEdge-IdTime-"+ dataset + "-" + trajId + "-" + k;
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new IdTemporalQuery(k, trajId, minLat, maxLat, minLng, maxLng, startTime, endTime), 1);
        builder.setBolt("idQuery", new QueryHandlerBolt(), 1).allGrouping("spout");


        if (!isCluster) {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology(topoName, config, builder.createTopology());
        } else {
            StormSubmitter.submitTopologyWithProgressBar(topoName, config, builder.createTopology());
        }
    }

}
