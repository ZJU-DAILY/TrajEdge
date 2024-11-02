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
import org.example.spout.query.SpacialRangeQuerySpout;
import org.example.spout.query.kNNQuerySpout;
import org.apache.storm.topology.TopologyBuilder;


/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class TrajectorykNNQueryTopology {

    public static void main(String[] args) throws Exception {
        boolean isCluster = false;
        String topoName = "test";
        String dataset = "geolife";
        String k = "8";
        String topk = "10";

        double minLat, maxLat, minLng, maxLng;
        minLat = 1.044024;
        maxLat = 63.0141583;
        minLng = -179.9695933;
        maxLng = 179.9969416;

        if (args.length > 0) {
            topoName = args[0];
            if (args.length > 1) {
                isCluster = true;
            }
            if (args.length > 2) {
                k = args[2];
            }
            if (args.length > 3) {
                dataset = args[3];
            }
            if (args.length > 3) {
                topk = args[4];
            }
        }

        Config config = new Config();
         if("tdrive".equals(dataset)){
            minLat = 0.0;
            maxLat = 65.20465;
            minLng = 0.0;
            maxLng = 174.06752;
        }

        config.setNumWorkers(1);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new kNNQuerySpout(Integer.parseInt(topk),Integer.parseInt(k), minLat, maxLat, minLng, maxLng), 1);
        builder.setBolt("spacialQuery", new QueryHandlerBolt(), 1).allGrouping("spout");

        topoName = "TrajEdge-knn-" + dataset + "-" + k + "-" + topk;
        if (!isCluster) {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology(topoName, config, builder.createTopology());
        } else {
            StormSubmitter.submitTopologyWithProgressBar(topoName, config, builder.createTopology());
        }
    }

}
