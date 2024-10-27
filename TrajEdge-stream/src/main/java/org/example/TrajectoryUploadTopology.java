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
import org.example.bolt.DataStoreBolt;
import org.example.spout.RandomPointSpout;
import org.example.spout.RandomTrajectorySpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;


/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class TrajectoryUploadTopology {

    public static void main(String[] args) throws Exception {
        System.setProperty("log4j.configurationFile", "log4j2.xml");
        boolean isCluster = false;
        String topoName = "null";
        String trajNum = "1000";
        if (args.length > 0) {
            topoName = args[0];
            if (args.length > 1) {
                isCluster = true;
            }
            if (args.length > 2) {
                trajNum = args[2];
            }
        }

        Config config = new Config();
        config.put("trajNum", trajNum);
        // if (!isCluster) {
        //     config.setDebug(false);
        //     config.put("data.src", "/home/hch/PROJECT/data/geolife/trajectory/");
        //     // config.put("data.index.dest", "C:\\Users\\HeAlec\\Desktop\\学院_实验室\\毕设\\代码\\TrajEdge\\output\\index\\");
        //     // config.put("data.dest", "C:\\Users\\HeAlec\\Desktop\\学院_实验室\\毕设\\代码\\TrajEdge\\output\\data\\");
        // }
        // else{
            
        //     // config.put("data.index.dest", "~/PROJECT/data/geolife/trajectory/");
        //     // config.put("data.dest", "C:\\Users\\HeAlec\\Desktop\\学院_实验室\\毕设\\代码\\TrajEdge\\output\\data\\");
        // }
        config.put("data.src", "/opt/data/trajectory/");
        config.setNumWorkers(1);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new RandomPointSpout(), 1);
        builder.setBolt("dataStore", new DataStoreBolt(), 1).fieldsGrouping("spout", new Fields("trajId"));

        if (!isCluster) {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology(topoName, config, builder.createTopology());
        } else {
            StormSubmitter.submitTopologyWithProgressBar(topoName, config, builder.createTopology());
        }


    }

}
