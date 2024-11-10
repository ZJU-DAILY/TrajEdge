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
    private String[] fileList;
    private int currentFileIndex = 0;
    private BufferedReader currentReader = null;
    private String dataSrc;
    private int sentTuples = 0;
    private boolean finished = false;

    // 平面坐标系范围
    private static final double X_MIN = 281;
    private static final double X_MAX = 23854;
    private static final double Y_MIN = 3935;
    private static final double Y_MAX = 30851;
    // 经纬度范围
    private static final double LON_MIN = 7.8;
    private static final double LON_MAX = 8.2;
    private static final double LAT_MIN = 53.0;
    private static final double LAT_MAX = 53.5;

    /**
     * 将平面坐标转换为经纬度
     */
    private double[] localToLatLon(double x, double y) {
        double longitude = LON_MIN + (x - X_MIN) / (X_MAX - X_MIN) * (LON_MAX - LON_MIN);
        double latitude = LAT_MIN + (y - Y_MIN) / (Y_MAX - Y_MIN) * (LAT_MAX - LAT_MIN);
        return new double[]{latitude, longitude};
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.dataSrc = (String) conf.get("data.src");
        this.collector = collector;
        File path = new File(this.dataSrc);
        fileList = path.list();
        LOG.info("Found {} files to process", fileList.length);
        openNextFile();
    }

    private void openNextFile() {
        try {
            if (currentReader != null) {
                currentReader.close();
            }
            
            while (currentFileIndex < fileList.length) {
                String filename = fileList[currentFileIndex];
                // 跳过不需要处理的文件
                // if (filename.equals("o5.dat")) {
                //     currentFileIndex++;
                //     continue;
                // }
                
                String filePath = dataSrc + filename;
                LOG.info("Opening file: {}", filePath);
                currentReader = new BufferedReader(new FileReader(filePath));
                return;
            }
            
            finished = true;
            LOG.info("All files have been processed");
        } catch (IOException e) {
            LOG.error("Error opening file: {}", e.getMessage());
            finished = true;
        }
    }

    @Override
    public void nextTuple() {
        if (finished) {
            Utils.sleep(100);
            return;
        }

        try {
            String line = currentReader.readLine();
            if (line == null) {
                currentFileIndex++;
                openNextFile();
                return;
            }

            // 解析原始数据行
            String[] parts = line.strip().split("\t");
            // LOG.info(parts[5]);
            // LOG.info(parts[6]);
            // LOG.info(parts[7]);
            if (parts.length >= 7) {  // 确保数据行包含足够的字段
                String id = parts[1];
                double x = Double.parseDouble(parts[5]);
                double y = Double.parseDouble(parts[6]);
                double timestamp = Double.parseDouble(parts[7]);
                long t = (long) timestamp;
                // 转换坐标
                double[] latLon = localToLatLon(x, y);
                
                Values tuple = new Values(Integer.valueOf(id), t, 0L, 0.0, latLon[0], latLon[1]);
                collector.emit(tuple, sentTuples);
                sentTuples++;
            }

        } catch (IOException e) {
            LOG.error("Error reading line: {}", e.getMessage());
            currentFileIndex++;
            openNextFile();
        } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
            LOG.error("Error parsing line: {}", e.getMessage());
        }
    }

    @Override
    public void ack(Object msgId) {
        // 可以添加确认处理逻辑
    }

    @Override
    public void fail(Object msgId) {
        // 可以添加失败重试逻辑
        LOG.error("Failed to process message ID: {}", msgId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("trajId", "timestamp", "edgeId", "dist", "oriLat", "oriLng"));
    }

    @Override
    public void close() {
        try {
            if (currentReader != null) {
                currentReader.close();
            }
        } catch (IOException e) {
            LOG.error("Error closing reader: {}", e.getMessage());
        }
    }
}

