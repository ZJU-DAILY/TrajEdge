package org.example.bolt;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.storm.Config;
import org.apache.storm.blobstore.AtomicOutputStream;
import org.apache.storm.blobstore.BlobStoreAclHandler;
import org.apache.storm.blobstore.LocalFsBlobStore;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyAlreadyExistsException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.SettableBlobMeta;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// "trajId", "edgeId", "dist"
public class IndexStoreBolt extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(IndexStoreBolt.class);
    private Map<String, Object> stormConf;
    private TopologyContext context;
    // 轨迹id的最大值
    private static Integer MAX_TRAJID_NUM = 5000;
    private LocalFsBlobStore store = null;

    private String index2dest;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.stormConf = stormConf;
        this.context = context;
        index2dest = (String) stormConf.get("data.index.dest");
        this.store = initLocalFs();
    }

    private LocalFsBlobStore initLocalFs() {
        LocalFsBlobStore store = new LocalFsBlobStore();

        Map<String, Object> conf = Utils.readStormConfig();
        conf.put(Config.STORM_ZOOKEEPER_PORT, this.stormConf.get("storm.zookeeper.port"));
        conf.put(Config.STORM_LOCAL_DIR, this.index2dest);
        conf.put(Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN, "org.apache.storm.security.auth.DefaultPrincipalToLocal");
        NimbusInfo nimbusInfo = new NimbusInfo("localhost", 0, false);
        store.prepare(conf, null, nimbusInfo, null);
        return store;
    }

    Set<Integer> deserialize(byte[] values) {
        Set<Integer> sets = new HashSet<>();
        int length = ByteBuffer.wrap(values, 0, 4).getInt();
        for (int i = 0; i < length; ++i) {
            int v = ByteBuffer.wrap(values, 4 + i * 4, 4).getInt();
            sets.add(v);
        }
        return sets;
    }

    byte[] serialize(Set<Integer> sets) {
        byte[] p = new byte[(sets.size() + 1) * 4];
        ByteBuffer values = ByteBuffer.wrap(p);
        values.putInt(sets.size());
        for (Integer v : sets) {
            values.putInt(v);
        }
        return p;
    }

    private void insert(Integer trajId, Long edgeId) throws AuthorizationException, KeyAlreadyExistsException {
        SettableBlobMeta metadata = new SettableBlobMeta(BlobStoreAclHandler
            .WORLD_EVERYTHING);
        try (AtomicOutputStream outputStream = store.createBlob(edgeId.toString(), metadata, null)) {
            Set<Integer> sets = new HashSet<>();
            sets.add(trajId);
            outputStream.write(serialize(sets));
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private void upsert(Integer trajId, Long edgeId)
        throws IOException, AuthorizationException, KeyAlreadyExistsException, KeyNotFoundException {
        // 1. read
        byte[] out = new byte[MAX_TRAJID_NUM * 4];
        Set<Integer> old = null;
        try (InputStream in = store.getBlob(edgeId.toString(), null)) {
            in.read(out);
            // 4.update
            old = deserialize(out);
            old.add(trajId);
            // 2. check
        } catch (KeyNotFoundException e) {
            // No this key, insert case.
            LOG.debug("edgeId {}, create {}", edgeId, trajId);
            // 3. write
            insert(trajId, edgeId);
        }
        if (old != null) {
            if (old.isEmpty()) {
                LOG.debug("edgeId {}, create {}, old set size {}", edgeId, trajId, old.size());
                insert(trajId, edgeId);
            } else {
                LOG.debug("edgeId {}, update {}, old set size {}", edgeId, trajId, old.size());
                try (AtomicOutputStream outputStream = store.updateBlob(edgeId.toString(), null)) {
                    outputStream.write(serialize(old));
                }
            }

        }
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        Long edgeId = tuple.getLongByField("edgeId");
        Integer trajId = tuple.getIntegerByField("trajId");
        try {
            upsert(trajId, edgeId);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
