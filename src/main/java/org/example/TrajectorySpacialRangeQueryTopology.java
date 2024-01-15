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
import org.example.bolt.QueryHandlerBolt;
import org.example.bolt.SpacialQueryBolt;
import org.example.spout.query.SpacialRangeQuerySpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;


/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class TrajectorySpacialRangeQueryTopology {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new SpacialRangeQuerySpout(), 1);
        builder.setBolt("SpacialQuery", new SpacialQueryBolt(), 1).fieldsGrouping("spout", new Fields("queryId"));
        builder.setBolt("StoreQuery", new QueryHandlerBolt(), 1).fieldsGrouping("SpacialQuery", new Fields("trajId"));

        Config config = new Config();
        config.setDebug(false);

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("trajectorySpacialRangeQuery", config, builder.createTopology());
    }

}
