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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.storm.metricstore.AggLevel;
import org.example.trajstore.TrajPoint;
import org.example.trajstore.TrajStoreException;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class designed to perform all metrics inserts into RocksDB.  Metrics are processed from a blocking queue.  Inserts
 * to RocksDB are done using a single thread to simplify design (such as looking up existing metric data for aggregation,
 * and fetching/evicting metadata from the cache).  This class is not thread safe.
 * </P>
 * A writable LRU StringMetadataCache is used to minimize looking up metadata string Ids.  As entries are added to the full cache, older
 * entries are evicted from the cache and need to be written to the database.  This happens as the handleEvictedMetadata()
 * method callback.
 * </P>
 * The following issues would need to be addressed to implement a multithreaded metrics writer:
 * <ul>
 *     <li>Generation of unique unused IDs for new metadata strings needs to be thread safe.</li>
 *     <li>Ensuring newly created metadata strings are seen by all threads.</li>
 *     <li>Maintaining a properly cached state of metadata for multiple writers.  The current LRU cache
 *     evicts data as new metadata is added.</li>
 *     <li>Processing the aggregation of a metric requires fetching and updating previous aggregates.  A multithreaded
 *     design would need to ensure two metrics were not updating an aggregated metric at the same time.</li>
 *     <li>Investigate performance of multiple threads inserting into RocksDB versus a single ordered insert.</li>
 * </ul>
 */
public class RocksDbTrajectoryWriter implements Runnable, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(RocksDbTrajectoryWriter.class);
    private RocksDbStore store;
    private BlockingQueue queue;
    private WritableStringMetadataCache stringMetadataCache;
    private Set<Integer> unusedIds = new HashSet<>();
    private TreeMap<RocksDbKey, RocksDbValue> insertBatch = new TreeMap<>(); // RocksDB should insert in sorted key order
    private WriteOptions writeOpts = new WriteOptions();
    private volatile boolean shutdown = false;
    private Meter failureMeter;
    private ArrayList<AggLevel> aggBuckets = new ArrayList<>();

    /**
     * Constructor for the RocksDbMetricsWriter.
     *
     * @param store The RocksDB store
     * @param queue The queue to receive metrics for insertion
     */
    RocksDbTrajectoryWriter(RocksDbStore store, BlockingQueue queue) {
        this.store = store;
        this.queue = queue;

        aggBuckets.add(AggLevel.AGG_LEVEL_1_MIN);
        aggBuckets.add(AggLevel.AGG_LEVEL_10_MIN);
        aggBuckets.add(AggLevel.AGG_LEVEL_60_MIN);
    }

    /**
     * Init routine called once the Metadata cache has been created.
     *
     * @throws TrajStoreException on cache error
     */
    void init() throws TrajStoreException {
        this.stringMetadataCache = StringMetadataCache.getWritableStringMetadataCache();
    }

    /**
     * Run routine to wait for metrics on a queue and insert into RocksDB.
     */
    @Override
    public void run() {
        while (!shutdown) {
            try {
                TrajPoint m = (TrajPoint) queue.take();
                processInsert(m);
            } catch (Exception e) {
                LOG.error("Failed to insert metric", e);
                if (this.failureMeter != null) {
                    this.failureMeter.mark();
                }
            }
        }
    }

    /**
     * Performs the actual metric insert, and aggregates over all bucket times.
     *
     * @throws TrajStoreException if database write fails
     */
    private void processInsert(TrajPoint point) throws TrajStoreException {

        // convert all strings to numeric Ids for the metric key and add to the metadata cache
//        long metricTimestamp = metric.getTimestamp();
//        Integer topologyId = storeMetadataString(KeyType.TRAJECTORY_STRING, point.getTrajId(), metricTimestamp);
//        Integer metricId = storeMetadataString(KeyType.METRIC_STRING, metric.getMetricName(), metricTimestamp);
//        Integer componentId = storeMetadataString(KeyType.COMPONENT_STRING, metric.getComponentId(), metricTimestamp);
//        Integer executorId = storeMetadataString(KeyType.EXEC_ID_STRING, metric.getExecutorId(), metricTimestamp);
//        Integer hostId = storeMetadataString(KeyType.HOST_STRING, metric.getHostname(), metricTimestamp);
//        Integer streamId = storeMetadataString(KeyType.STREAM_ID_STRING, metric.getStreamId(), metricTimestamp);

        RocksDbKey key = RocksDbKey.createTrajPointKey(point.getTrajId(), point.getTimestamp());

        // save metric key/value to be batched
        RocksDbValue value = new RocksDbValue(point);
        insertBatch.put(key, value);

        processBatchInsert(insertBatch);

        insertBatch.clear();
    }

    // converts a metadata string into a unique integer.  Updates the timestamp of the string
    // so we can track when it was last used for later deletion on database cleanup.
    private int storeMetadataString(KeyType type, String s) throws TrajStoreException {
        if (s == null) {
            throw new TrajStoreException("No string for metric metadata string type " + type);
        }

        // attempt to find it in the string cache. We promise no upsert.
        StringMetadata stringMetadata = stringMetadataCache.get(s);
        if (stringMetadata != null) {
            return stringMetadata.getStringId();
        }

        // attempt to find the string in the database
        try {
            stringMetadata = store.rocksDbGetStringMetadata(type, s);
        } catch (RocksDBException e) {
            throw new TrajStoreException("Error reading metrics data", e);
        }
        if (stringMetadata != null) {
            // update to the latest timestamp and add to the string cache
            stringMetadataCache.put(s, stringMetadata, false);
            return stringMetadata.getStringId();
        }

        // string does not exist, create using an unique string id and add to cache
        if (LOG.isDebugEnabled()) {
            LOG.debug(type + "." + s + " does not exist in cache or database");
        }
        int stringId = getUniqueMetadataStringId();
        stringMetadata = new StringMetadata(type, stringId);
        stringMetadataCache.put(s, stringMetadata, true);

        return stringMetadata.getStringId();
    }

    // get a currently unused unique string id
    private int getUniqueMetadataStringId() throws TrajStoreException {
        generateUniqueStringIds();
        int id = unusedIds.iterator().next();
        unusedIds.remove(id);
        return id;
    }

    // guarantees a list of unused string Ids exists.  Once the list is empty, creates a new list
    // by generating a list of random numbers and removing the ones that already are in use.
    private void generateUniqueStringIds() throws TrajStoreException {
        int attempts = 0;
        while (unusedIds.isEmpty()) {
            attempts++;
            if (attempts > 100) {
                String message = "Failed to generate unique ids";
                LOG.error(message);
                throw new TrajStoreException(message);
            }
            for (int i = 0; i < 600; i++) {
                int n = ThreadLocalRandom.current().nextInt();
                if (n == RocksDbStore.INVALID_METADATA_STRING_ID) {
                    continue;
                }
                // remove any entries in the cache
                if (stringMetadataCache.contains(n)) {
                    continue;
                }
                unusedIds.add(n);
            }
            // now scan all metadata and remove any matching string Ids from this list
            RocksDbKey firstPrefix = RocksDbKey.getPrefix(KeyType.METADATA_STRING_START);
            RocksDbKey lastPrefix = RocksDbKey.getPrefix(KeyType.METADATA_STRING_END);
            try {
                store.scanRange(firstPrefix, lastPrefix, (key, value) -> {
                    unusedIds.remove(key.getMetadataStringId());
                    return true; // process all metadata
                });
            } catch (RocksDBException e) {
                throw new TrajStoreException("Error reading metrics data", e);
            }
        }
    }

    // writes multiple metric values into the database as a batch operation.  The tree map keeps the keys sorted
    // for faster insertion to RocksDB.
    private void processBatchInsert(TreeMap<RocksDbKey, RocksDbValue> batchMap) throws TrajStoreException {
        try (WriteBatch writeBatch = new WriteBatch()) {
            // take the batched metric data and write to the database
            for (RocksDbKey k : batchMap.keySet()) {
                RocksDbValue v = batchMap.get(k);
                writeBatch.put(k.getRaw(), v.getRaw());
            }
            store.db.write(writeOpts, writeBatch);
        } catch (Exception e) {
            String message = "Failed to store data to RocksDB";
            LOG.error(message, e);
            throw new TrajStoreException(message, e);
        }
    }

    // evicted metadata needs to be stored immediately.  Metadata lookups count on it being in the cache
    // or database.
    void handleEvictedMetadata(RocksDbKey key, RocksDbValue val) {
        try {
            store.db.put(key.getRaw(), val.getRaw());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    boolean isShutdown() {
        return this.shutdown;
    }

    @Override
    public void close() {
        this.shutdown = true;

        // get all metadata from the cache to put into the database
        TreeMap<RocksDbKey, RocksDbValue> batchMap = new TreeMap<>();  // use a new map to prevent threading issues with writer thread
        for (Map.Entry entry : stringMetadataCache.entrySet()) {
            String metadataString = (String) entry.getKey();
            StringMetadata val = (StringMetadata) entry.getValue();
            RocksDbValue rval = new RocksDbValue(metadataString);

            for (KeyType type : val.getMetadataTypes()) {   // save the metadata for all types of strings it matches
                RocksDbKey rkey = new RocksDbKey(type, val.getStringId());
                batchMap.put(rkey, rval);
            }
        }

        try {
            processBatchInsert(batchMap);
        } catch (TrajStoreException e) {
            LOG.error("Failed to insert all metadata", e);
        }

        // flush db to disk
        try (FlushOptions flushOps = new FlushOptions()) {
            flushOps.setWaitForFlush(true);
            store.db.flush(flushOps);
        } catch (RocksDBException e) {
            LOG.error("Failed ot flush RocksDB", e);
            if (this.failureMeter != null) {
                this.failureMeter.mark();
            }
        }
    }
}
