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

import com.codahale.metrics.Meter;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.storm.DaemonConfig;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.ObjectReader;
import org.example.trajstore.FilterOptions;
import org.example.trajstore.TrajPoint;
import org.example.trajstore.TrajStore;
import org.example.trajstore.TrajStoreException;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.IndexType;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RocksDbStore implements TrajStore, AutoCloseable {
    static final int INVALID_METADATA_STRING_ID = 0;
    private static final Logger LOG = LoggerFactory.getLogger(RocksDbStore.class);
    private static final int MAX_QUEUE_CAPACITY = 4000;
    RocksDB db;
    private ReadOnlyStringMetadataCache readOnlyStringMetadataCache = null;
    private BlockingQueue queue = new LinkedBlockingQueue(MAX_QUEUE_CAPACITY);
    private RocksDbTrajectoryWriter metricsWriter = null;
    private Meter failureMeter = null;

    @Override
    public void prepare(Map<String, Object> config) throws TrajStoreException {
        validateConfig(config);


        RocksDB.loadLibrary();
        boolean createIfMissing = ObjectReader.getBoolean(config.get(DaemonConfig.STORM_ROCKSDB_CREATE_IF_MISSING), false);

        try (Options options = new Options().setCreateIfMissing(createIfMissing)) {
            // use the hash index for prefix searches
            BlockBasedTableConfig tfc = new BlockBasedTableConfig();
            tfc.setIndexType(IndexType.kHashSearch);
            options.setTableFormatConfig(tfc);
            options.useCappedPrefixExtractor(RocksDbKey.KEY_SIZE);

            String path = getRocksDbAbsoluteDir(config);
            LOG.info("Opening RocksDB from {}, {}={}", path, DaemonConfig.STORM_ROCKSDB_CREATE_IF_MISSING, createIfMissing);
            db = RocksDB.open(options, path);
        } catch (RocksDBException e) {
            String message = "Error opening RockDB database";
            LOG.error(message, e);
            throw new TrajStoreException(message, e);
        }


        // create thread to process insertion of all metrics
        metricsWriter = new RocksDbTrajectoryWriter(this, this.queue);

        int cacheCapacity = Integer.parseInt(config.get(DaemonConfig.STORM_ROCKSDB_METADATA_STRING_CACHE_CAPACITY).toString());
        StringMetadataCache.init(metricsWriter, cacheCapacity);
        readOnlyStringMetadataCache = StringMetadataCache.getReadOnlyStringMetadataCache();
        metricsWriter.init(); // init the writer once the cache is setup

        Thread thread = new Thread(metricsWriter, "RocksDbMetricsWriter");
        thread.setDaemon(true);
        thread.start();
    }

    private void validateConfig(Map<String, Object> config) throws TrajStoreException {
        if (!(config.containsKey(DaemonConfig.STORM_ROCKSDB_LOCATION))) {
            throw new TrajStoreException(
                "Not a vaild RocksDB configuration - Missing store location " + DaemonConfig.STORM_ROCKSDB_LOCATION);
        }

        if (!(config.containsKey(DaemonConfig.STORM_ROCKSDB_CREATE_IF_MISSING))) {
            throw new TrajStoreException("Not a vaild RocksDB configuration - Does not specify creation policy "
                + DaemonConfig.STORM_ROCKSDB_CREATE_IF_MISSING);
        }

        // validate path defined
        String storePath = getRocksDbAbsoluteDir(config);

        boolean createIfMissing = ObjectReader.getBoolean(config.get(DaemonConfig.STORM_ROCKSDB_CREATE_IF_MISSING), false);
        if (!createIfMissing) {
            if (!(new File(storePath).exists())) {
                throw new TrajStoreException("Configuration specifies not to create a store but no store currently exists at " + storePath);
            }
        }

        if (!(config.containsKey(DaemonConfig.STORM_ROCKSDB_METADATA_STRING_CACHE_CAPACITY))) {
            throw new TrajStoreException("Not a valid RocksDB configuration - Missing metadata string cache size "
                + DaemonConfig.STORM_ROCKSDB_METADATA_STRING_CACHE_CAPACITY);
        }

        if (!config.containsKey(DaemonConfig.STORM_ROCKSDB_METRIC_RETENTION_HOURS)) {
            throw new TrajStoreException("Not a valid RocksDB configuration - Missing metric retention "
                + DaemonConfig.STORM_ROCKSDB_METRIC_RETENTION_HOURS);
        }
    }

    private String getRocksDbAbsoluteDir(Map<String, Object> conf) throws TrajStoreException {
        String storePath = (String) conf.get(DaemonConfig.STORM_ROCKSDB_LOCATION);
        if (storePath == null) {
            throw new TrajStoreException(
                "Not a vaild RocksDB configuration - Missing store location " + DaemonConfig.STORM_ROCKSDB_LOCATION);
        } else {
            if (new File(storePath).isAbsolute()) {
                return storePath;
            } else {
                String stormHome = System.getProperty(ConfigUtils.STORM_HOME);
                if (stormHome == null) {
                    throw new TrajStoreException(ConfigUtils.STORM_HOME + " not set");
                }
                return (stormHome + File.separator + storePath);
            }
        }
    }

    @Override
    public void insert(TrajPoint point) throws TrajStoreException {
        try {
            // don't bother blocking on a full queue, just drop metrics in case we can't keep up
            if (queue.remainingCapacity() <= 0) {
                LOG.info("Metrics q full, dropping metric");
                return;
            }
            queue.put(point);
        } catch (Exception e) {
            String message = "Failed to insert metric";
            LOG.error(message, e);
            if (this.failureMeter != null) {
                this.failureMeter.mark();
            }
            throw new TrajStoreException(message, e);
        }
    }

    @Override
    public boolean populateValue(TrajPoint point) throws TrajStoreException {

        RocksDbKey key = RocksDbKey.createTrajPointKey(point.getTrajId(), point.getTimestamp());

        return populateFromKey(key, point);
    }

    // populate metric values using the provided key
    boolean populateFromKey(RocksDbKey key, TrajPoint point) throws TrajStoreException {
        try {
            byte[] value = db.get(key.getRaw());
            if (value == null) {
                LOG.info("value is null");
                return false;
            }
            RocksDbValue rdbValue = new RocksDbValue(value);
            rdbValue.populateMetric(point);
        } catch (Exception e) {
            String message = "Failed to populate metric";
            LOG.error(message, e);
            if (this.failureMeter != null) {
                this.failureMeter.mark();
            }
            throw new TrajStoreException(message, e);
        }
        return true;
    }

    // attempts to lookup the unique Id for a string that may not exist yet.  Returns INVALID_METADATA_STRING_ID
    // if it does not exist.
//    private int lookupMetadataString(KeyType type, String s, Map<String, Integer> lookupCache) throws TrajStoreException {
//        if (s == null) {
//            if (this.failureMeter != null) {
//                this.failureMeter.mark();
//            }
//            throw new TrajStoreException("No string for metric metadata string type " + type);
//        }
//
//        // attempt to find it in the string cache, this will update the LRU
//        StringMetadata stringMetadata = readOnlyStringMetadataCache.get(s);
//        if (stringMetadata != null) {
//            return stringMetadata.getStringId();
//        }
//
//        // attempt to find it in callers cache
//        Integer id = lookupCache.get(s);
//        if (id != null) {
//            return id;
//        }
//
//        // attempt to find the string in the database
//        try {
//            stringMetadata = rocksDbGetStringMetadata(type, s);
//        } catch (RocksDBException e) {
//            throw new TrajStoreException("Error reading metric data", e);
//        }
//
//        if (stringMetadata != null) {
//            id = stringMetadata.getStringId();
//
//            // add to the callers cache.  We can't add it to the stringMetadataCache, since that could cause an eviction
//            // database write, which we want to only occur from the inserting DB thread.
//            lookupCache.put(s, id);
//
//            return id;
//        }
//
//        // string does not exist
//        return INVALID_METADATA_STRING_ID;
//    }

    // scans the database to look for a metadata string and returns the metadata info
    StringMetadata rocksDbGetStringMetadata(KeyType type, String s) throws RocksDBException {
        RocksDbKey firstKey = RocksDbKey.getInitialKey(type);
        RocksDbKey lastKey = RocksDbKey.getLastKey(type);
        final AtomicReference<StringMetadata> reference = new AtomicReference<>();
        scanRange(firstKey, lastKey, (key, value) -> {
            if (s.equals(value.getMetdataString())) {
                reference.set(value.getStringMetadata(key));
                return false;
            } else {
                return true;  // haven't found string, keep searching
            }
        });
        return reference.get();
    }

    // scans from key start to the key before end, calling back until callback indicates not to process further
    void scanRange(RocksDbKey start, RocksDbKey end, RocksDbScanCallback fn) throws RocksDBException {
        try (ReadOptions ro = new ReadOptions()) {
            ro.setTotalOrderSeek(true);
            try (RocksIterator iterator = db.newIterator(ro)) {
                for (iterator.seek(start.getRaw()); iterator.isValid(); iterator.next()) {
                    RocksDbKey key = new RocksDbKey(iterator.key());
                    if (key.compareTo(end) >= 0) { // past limit, quit
                        return;
                    }

                    RocksDbValue val = new RocksDbValue(iterator.value());
                    if (!fn.cb(key, val)) {
                        // if cb returns false, we are done with this section of rows
                        return;
                    }
                }
            }
        }
    }

    /**
     * Shutdown the store.
     */
    @Override
    public void close() {
        metricsWriter.close();
    }

    @Override
    public void scan(FilterOptions filter, ScanCallback scanCallback) throws TrajStoreException {
        scanInternal(filter, scanCallback, null);
    }

    private void scanRaw(FilterOptions filter, RocksDbScanCallback rawCallback) throws TrajStoreException {
        scanInternal(filter, null, rawCallback);
    }

    // perform a scan given filter options, and return results in either Metric or raw data.
    private void scanInternal(FilterOptions filter, ScanCallback scanCallback, RocksDbScanCallback rawCallback) throws TrajStoreException {

        Map<String, Integer> stringToIdCache = new HashMap<>();
        Map<Integer, String> idToStringCache = new HashMap<>();

        int startTrajId = 0;
        int endTrajId = 0xFFFFFFFF;
        Integer traId = filter.getTrajectoryId();
        if (traId != null) {
            startTrajId = traId;
            endTrajId = traId;
        }

        long startTime = filter.getStartTime();
        long endTime = filter.getEndTime();

        try (ReadOptions ro = new ReadOptions()) {
            ro.setTotalOrderSeek(true);
            RocksDbKey startKey = RocksDbKey.createTrajPointKey(startTrajId, startTime);
            RocksDbKey endKey = RocksDbKey.createTrajPointKey(endTrajId, endTime);

            try (RocksIterator iterator = db.newIterator(ro)) {
                for (iterator.seek(startKey.getRaw()); iterator.isValid(); iterator.next()) {
                    RocksDbKey key = new RocksDbKey(iterator.key());

                    if (key.compareTo(endKey) > 0) { // past limit, quit
                        break;
                    }

                    if (startTrajId != 0 && key.getTrajId() != startTrajId) {
                        continue;
                    }

                    long timestamp = key.getTimestamp();
                    if (timestamp < startTime || timestamp > endTime) {
                        continue;
                    }


                    RocksDbValue val = new RocksDbValue(iterator.value());

                    if (scanCallback != null) {
                        // Fake point
                        TrajPoint point = new TrajPoint(key.getTrajId(), timestamp, val.getEdgeId(), val.getDistance(),
                         val.getOriLat(), val.getOriLng());
                        
                        val.populateMetric(point);

                        // callback to caller
                        scanCallback.cb(point);
                    } else {
                        try {
                            if (!rawCallback.cb(key, val)) {
                                return;
                            }
                        } catch (RocksDBException e) {
                            throw new TrajStoreException("Error reading metrics data", e);
                        }
                    }
                }
            }
        }
    }
    // deletes metrics matching the filter options
//    void deleteMetrics(FilterOptions filter) throws TrajStoreException {
//        try (WriteBatch writeBatch = new WriteBatch();
//             WriteOptions writeOps = new WriteOptions()) {
//
//            scanRaw(filter, (RocksDbKey key, RocksDbValue value) -> {
//                writeBatch.delete(key.getRaw());
//                return true;
//            });
//
//            if (writeBatch.count() > 0) {
//                LOG.info("Deleting {} metrics", writeBatch.count());
//                try {
//                    db.write(writeOps, writeBatch);
//                } catch (Exception e) {
//                    String message = "Failed delete metrics";
//                    LOG.error(message, e);
//                    if (this.failureMeter != null) {
//                        this.failureMeter.mark();
//                    }
//                    throw new TrajStoreException(message, e);
//                }
//            }
//        }
//    }

    // deletes metadata strings before the provided timestamp
//    void deleteMetadataBefore(long firstValidTimestamp) throws TrajStoreException {
//        if (firstValidTimestamp < 1L) {
//            if (this.failureMeter != null) {
//                this.failureMeter.mark();
//            }
//            throw new TrajStoreException("Invalid timestamp for deleting metadata: " + firstValidTimestamp);
//        }
//
//        try (WriteBatch writeBatch = new WriteBatch();
//             WriteOptions writeOps = new WriteOptions()) {
//
//            // search all metadata strings
//            RocksDbKey topologyMetadataPrefix = RocksDbKey.getPrefix(KeyType.METADATA_STRING_START);
//            RocksDbKey lastPrefix = RocksDbKey.getPrefix(KeyType.METADATA_STRING_END);
//            try {
//                scanRange(topologyMetadataPrefix, lastPrefix, (key, value) -> {
//                    // we'll assume the metadata was recently used if still in the cache.
//                    if (!readOnlyStringMetadataCache.contains(key.getMetadataStringId())) {
//                        if (value.getLastTimestamp() < firstValidTimestamp) {
//                            writeBatch.delete(key.getRaw());
//                        }
//                    }
//                    return true;
//                });
//            } catch (RocksDBException e) {
//                throw new TrajStoreException("Error reading metric data", e);
//            }
//
//            if (writeBatch.count() > 0) {
//                LOG.info("Deleting {} metadata strings", writeBatch.count());
//                try {
//                    db.write(writeOps, writeBatch);
//                } catch (Exception e) {
//                    String message = "Failed delete metadata strings";
//                    LOG.error(message, e);
//                    if (this.failureMeter != null) {
//                        this.failureMeter.mark();
//                    }
//                    throw new TrajStoreException(message, e);
//                }
//            }
//        }
//    }

    interface RocksDbScanCallback {
        boolean cb(RocksDbKey key, RocksDbValue val) throws RocksDBException;  // return false to stop scan
    }
}

