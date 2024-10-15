package org.example.bolt;

import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.DaemonConfig;
import org.example.trajstore.TrajPoint;
import org.example.trajstore.TrajStore;
import org.example.trajstore.TrajStoreConfig;
import org.example.trajstore.TrajStoreException;
import org.example.trajstore.rocksdb.StringMetadataCache;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// "trajId", "edgeId", "dist"
public class DataStoreBolt extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(DataStoreBolt.class);
    private PrintWriter writer;
    private Map<String, Object> stormConf;
    private TopologyContext context;
    private TrajStore store;
    private Path tempDirForStore;
    private int processedTuples = 0;
    private static final int LOG_INTERVAL = 1000; // 每处理1000个tuple记录一次日志

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.stormConf = stormConf;
        this.context = context;
        try {
            String data2dest = (String) this.stormConf.get("data.dest");
            StringMetadataCache.cleanUp();
            tempDirForStore = Path.of(data2dest);
            Map<String, Object> conf = new HashMap<>();
            conf.put(DaemonConfig.STORM_METRIC_STORE_CLASS, "org.example.trajstore.rocksdb.RocksDbStore");
            conf.put(DaemonConfig.STORM_ROCKSDB_LOCATION, tempDirForStore.toString());
            conf.put(DaemonConfig.STORM_ROCKSDB_CREATE_IF_MISSING, true);
            conf.put(DaemonConfig.STORM_ROCKSDB_METADATA_STRING_CACHE_CAPACITY, 4000);
            conf.put(DaemonConfig.STORM_ROCKSDB_METRIC_RETENTION_HOURS, 240);
            store = TrajStoreConfig.configure(conf);

        } catch (TrajStoreException e) {
            e.printStackTrace();
        }
        LOG.debug("data store is prepared...");
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Integer trajId = input.getIntegerByField("trajId");
        Long timestamp = input.getLongByField("timestamp");
        Long edgeId = input.getLongByField("edgeId");
        Double dist = input.getDoubleByField("dist");

        TrajPoint p = new TrajPoint(trajId, timestamp, edgeId, dist);
        try {
            synchronized (store){
                store.insert(p);
            }
            LOG.debug(p.toString());
        } catch (TrajStoreException e) {
            e.printStackTrace();
        }

        processedTuples++;
        if (processedTuples % LOG_INTERVAL == 0) {
            LOG.info("DataStoreBolt - Processed tuples: " + processedTuples);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }

    @Override
    public void cleanup() {
        if (store != null) {
            store.close();
        }
        StringMetadataCache.cleanUp();
    }

}
