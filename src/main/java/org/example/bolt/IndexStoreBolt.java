package org.example.bolt;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class IndexStoreBolt extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(IndexStoreBolt.class);
    private Map<String, Object> stormConf;
    private TopologyContext context;
    private static final int MAX_TRAJID_NUM = 5000;
    private RocksDB db;
    private String dbPath;

    private ConcurrentHashMap<Long, Set<Integer>> inMemoryIndex;
    private ScheduledExecutorService scheduler;
    private static final int FLUSH_INTERVAL_SECONDS = 60;
    private static final int BATCH_SIZE = 1000;
    private AtomicLong processedTuples;
    private static final int LOG_INTERVAL = 10000; // Log every 10000 tuples

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.stormConf = stormConf;
        this.context = context;
        this.dbPath = (String) stormConf.get("data.index.dest");
        this.inMemoryIndex = new ConcurrentHashMap<>();
        this.scheduler = Executors.newScheduledThreadPool(1);
        this.processedTuples = new AtomicLong(0);

        initRocksDB();
        scheduler.scheduleAtFixedRate(this::flushToDisk, FLUSH_INTERVAL_SECONDS, FLUSH_INTERVAL_SECONDS, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(this::logProgress, 10, 10, TimeUnit.SECONDS);
    }

    private void initRocksDB() {
        try {
            Options options = new Options().setCreateIfMissing(true);
            db = RocksDB.open(options, dbPath);
        } catch (RocksDBException e) {
            LOG.error("Failed to initialize RocksDB", e);
            throw new RuntimeException(e);
        }
    }

    private Set<Integer> deserialize(byte[] values) {
        Set<Integer> sets = new HashSet<>();
        ByteBuffer buffer = ByteBuffer.wrap(values);
        int length = buffer.getInt();
        for (int i = 0; i < length; i++) {
            sets.add(buffer.getInt());
        }
        return sets;
    }

    private byte[] serialize(Set<Integer> sets) {
        ByteBuffer buffer = ByteBuffer.allocate((sets.size() + 1) * 4);
        buffer.putInt(sets.size());
        for (Integer v : sets) {
            buffer.putInt(v);
        }
        return buffer.array();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        Long edgeId = tuple.getLongByField("edgeId");
        Integer trajId = tuple.getIntegerByField("trajId");
        
        inMemoryIndex.computeIfAbsent(edgeId, k -> ConcurrentHashMap.newKeySet()).add(trajId);
        
        long count = processedTuples.incrementAndGet();
        if (count % LOG_INTERVAL == 0) {
            LOG.info("Processed {} tuples", count);
        }
        
        if (inMemoryIndex.size() >= BATCH_SIZE) {
            flushToDisk();
        }
    }

    private void flushToDisk() {
        try (WriteBatch batch = new WriteBatch()) {
            for (Map.Entry<Long, Set<Integer>> entry : inMemoryIndex.entrySet()) {
                Long edgeId = entry.getKey();
                Set<Integer> newTrajIds = entry.getValue();
                
                byte[] key = edgeId.toString().getBytes();
                byte[] existingValue = db.get(key);
                Set<Integer> existingTrajIds = existingValue != null ? deserialize(existingValue) : new HashSet<>();
                
                existingTrajIds.addAll(newTrajIds);
                batch.put(key, serialize(existingTrajIds));
            }
            db.write(new WriteOptions(), batch);
            inMemoryIndex.clear();
        } catch (RocksDBException e) {
            LOG.error("Error flushing to RocksDB", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public void cleanup() {
        flushToDisk();
        scheduler.shutdown();
        try {
            scheduler.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.error("Error shutting down scheduler", e);
        }
        if (db != null) {
            db.close();
        }
    }

    private void logProgress() {
        LOG.info("Total processed tuples: {}, Current in-memory index size: {}", 
                 processedTuples.get(), inMemoryIndex.size());
    }
}
