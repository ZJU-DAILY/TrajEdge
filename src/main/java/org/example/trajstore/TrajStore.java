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

package org.example.trajstore;

import java.util.Map;

public interface TrajStore extends AutoCloseable {

    void prepare(Map<String, Object> config) throws TrajStoreException;

    void insert(TrajPoint point) throws TrajStoreException;

    boolean populateValue(TrajPoint point) throws TrajStoreException;

    @Override
    void close();

    void scan(FilterOptions filter, ScanCallback scanCallback) throws TrajStoreException;

    interface ScanCallback {
        void cb(TrajPoint point);
    }
}




