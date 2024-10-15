package org.example.bolt;

import com.esotericsoftware.minlog.Log;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.storm.DaemonConfig;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.example.dht.Helper;
import org.example.dht.Request;
import org.example.trajstore.TrajPoint;
import org.example.trajstore.TrajStore;
import org.example.trajstore.TrajStoreConfig;
import org.example.trajstore.TrajStoreException;
import org.example.trajstore.rocksdb.StringMetadataCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// "trajId", "edgeId", "dist"
public class DataStoreofDHTBolt extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(DataStoreofDHTBolt.class);
    private List<String> hosts;
    private static InetSocketAddress localAddress;
    private static Helper helper;
    private int processedTuples = 0;
    private static final int LOG_INTERVAL = 1000;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        hosts = new ArrayList<>();
        helper = new Helper();
        int nodeNum = 10, startPort = 10100;
        String local_ip = null;
        try {
            local_ip = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        for(int i = startPort; i < startPort + nodeNum; ++i){
            hosts.add(local_ip + ":" + i);
        }
        Random random = new Random();
        int randomNumber = random.nextInt(hosts.size());

        String host = hosts.get(randomNumber);
        localAddress = Helper.createSocketAddress(host);
        if (localAddress == null) {
            LOG.error("Cannot find address you are trying to contact. Now exit.");
        }
        // successfully constructed socket address of the node we are
        // trying to contact, check if it's alive
        String response = Helper.sendRequest(localAddress, new Request("KEEP", null , Request.DataType.None));

        // if it's dead, exit
        if (response == null || !response.equals("ALIVE"))  {
            LOG.error("\nCannot find node you are trying to contact. Now exit.\n");
        }

        // it's alive, print connection info
        LOG.info("Connection to node " + localAddress.getAddress().toString() + ", port " + localAddress.getPort() + ", position " +
            Helper.hexIdAndPosition(localAddress) + ".");
        LOG.debug("data store is prepared...");
    }

    private boolean checkStatus(){
        // check if system is stable
        boolean pred = false;
        boolean succ = false;
        InetSocketAddress pred_addr = Helper.requestAddress(localAddress, new Request("YOURPRE", null, Request.DataType.None));
        InetSocketAddress succ_addr = Helper.requestAddress(localAddress, new Request("YOURSUCC", null, Request.DataType.None));
        if (pred_addr == null || succ_addr == null) {
            LOG.warn("The node your are contacting is disconnected. Now exit.");
            return false;
        }
        if (pred_addr.equals(localAddress))
            pred = true;
        if (succ_addr.equals(localAddress))
            succ = true;

        // we suppose the system is stable if (1) this node has both valid
        // predecessor and successor or (2) none of them
        while (pred^succ) {
            LOG.info("Waiting for the system to be stable...");
            pred_addr = Helper.requestAddress(localAddress, new Request("YOURPRE", null, Request.DataType.None));
            succ_addr = Helper.requestAddress(localAddress, new Request("YOURSUCC", null, Request.DataType.None));
            if (pred_addr == null || succ_addr == null) {
                LOG.error("The node your are contacting is disconnected. Now exit.");
                return false;
            }
            pred = pred_addr.equals(localAddress);
            succ = succ_addr.equals(localAddress);
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
            }
        }
        return true;
    }
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if(!checkStatus())return;
        Integer trajId = input.getIntegerByField("trajId");
        Long timestamp = input.getLongByField("timestamp");
        Long edgeId = input.getLongByField("edgeId");
        Double dist = input.getDoubleByField("dist");

        TrajPoint p = new TrajPoint(trajId, timestamp, edgeId, dist);

        // Find which node to store
        long hash = Helper.hashString(String.valueOf(trajId));
        InetSocketAddress result = Helper.requestAddress(localAddress, new Request("FINDSUCC", hash, Request.DataType.ID));

        // if fail to send request, local node is disconnected, exit
        if (result == null) {
            LOG.warn("The node your are contacting is disconnected. Now exit.");
            return;
        }
        LOG.debug("Node "+result.getAddress().toString()+", port "+result.getPort()+", position "+Helper.hexIdAndPosition(result ));

        InetSocketAddress dataHostAddress = Helper.createSocketAddress(result.getAddress().toString().substring(1)+":"+result.getPort());
        String response = Helper.sendRequest(dataHostAddress, new Request("STORE", p, Request.DataType.Point));
        if(response != null){
            LOG.debug(trajId + " " + response);
        }

        processedTuples++;
        if (processedTuples % LOG_INTERVAL == 0) {
            LOG.info("DataStoreofDHTBolt - Processed tuples: " + processedTuples);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }

}
