/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.example.trajstore.rocksdb;

import java.util.ArrayList;
import java.util.List;

/**
 * Class that contains the information associated with a metadata string that remains cached in memory.
 */
class StringMetadata {
    private List<KeyType> types = new ArrayList<>(1);  // its possible a string is used by multiple types of metadata strings
    private int stringId;

    /**
     * Constructor for StringMetadata.
     *
     * @param metadataType the type of metadata string
     * @param stringId     the unique id for the metadata string
     */
    StringMetadata(KeyType metadataType, Integer stringId) {
        this.types.add(metadataType);
        this.stringId = stringId;
    }

    int getStringId() {
        return this.stringId;
    }

    List<KeyType> getMetadataTypes() {
        return this.types;
    }

    private void addKeyType(KeyType type) {
        if (!this.types.contains(type)) {
            this.types.add(type);
        }
    }

//    /**
//     * Updates the timestamp of when a metadata string was last used.  Adds the type of the string if it is a new
//     * type. 这里的timestamp还是加上吧，主要用于stringBuffer中的数据更新，rocksdb不感知这个值。
//     *
//     * @param metricTimestamp the timestamp of the metric using the metadata string
//     * @param type            the type of metadata string for the metric
//     */
//    void update(Long metricTimestamp, KeyType type) {
//        if (metricTimestamp > this.lastTimestamp) {
//            this.lastTimestamp = metricTimestamp;
//        }
//        addKeyType(type);
//    }

}

