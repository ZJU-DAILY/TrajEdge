package org.example.bolt;

import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.DaemonConfig;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.example.trajstore.FilterOptions;
import org.example.trajstore.TrajPoint;
import org.example.trajstore.TrajStore;
import org.example.trajstore.TrajStoreConfig;
import org.example.trajstore.TrajStoreException;
import org.example.trajstore.rocksdb.StringMetadataCache;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author alecHe
 * @desc ...
 * @date 2023-11-22 15:59:05
 */
public class QueryHandlerBolt extends BaseBasicBolt {

    private static final Logger LOG = LoggerFactory.getLogger(DataStoreBolt.class);
    private PrintWriter writer;
    private Map<String, Object> stormConf;
    private TopologyContext context;
    private TrajStore store;
    private Path tempDirForStore;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.stormConf = stormConf;
        this.context = context;
        try {
            StringMetadataCache.cleanUp();
            tempDirForStore = Path.of((String) stormConf.get("data.dest"));
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
    }

    private List<TrajPoint> getMetricsFromScan(FilterOptions filter) throws TrajStoreException {
        List<TrajPoint> list = new ArrayList<>();
        store.scan(filter, list::add);
        return list;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Integer trajId = input.getIntegerByField("trajId");
        Long startTime = input.getLongByField("startTime");
        Long endTime = input.getLongByField("endTime");
        List<TrajPoint> list;
        FilterOptions filter = new FilterOptions();

        if (trajId != -1) {
            filter.setTrajectoryId(trajId);
        }
        if (startTime != -1) {
            filter.setStartTime(startTime);
        }
        if (endTime != -1) {
            filter.setEndTime(endTime);
        }

        try {
            list = getMetricsFromScan(filter);
        } catch (TrajStoreException e) {
            e.printStackTrace();
            return;
        }
        if (list.isEmpty()) {
            LOG.info("There is no trajectory {}.", trajId);
            return;
        }
        for (TrajPoint p : list) {
            LOG.info(p.toString());
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
