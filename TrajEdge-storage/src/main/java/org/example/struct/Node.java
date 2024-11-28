package org.example.struct;
import java.util.List;

import org.example.trajstore.TrajStore;
import org.example.trajstore.TrajStoreConfig;
import org.example.trajstore.TrajStoreException;
import org.example.trajstore.TrajPoint;
import org.example.NodesService;
import org.example.trajstore.FilterOptions;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.DaemonConfig;
import java.util.HashMap;
import java.util.Map;
import java.io.File;

// 在实现中进行简化,不为时间key设立匹配规则,统一走rocksdb过滤
public class Node{
    private static final Logger LOG = LoggerFactory.getLogger(Node.class);
    private static final String projectPath = new File(".").getAbsolutePath();

    
    private String dockerName;
    private String prefix;
    // child/neigh/parent prefix => docker name
    private Map<String, String> routingEntry;
    private List<Integer> trajIds;
    private TrajStore store;

    public Node(String prefix) {
        this.prefix = prefix;
    }

    public Node(String prefix, Map<String, String> routingEntry, String dockerName) {
        this.dockerName = dockerName;
        this.prefix = prefix;
        this.routingEntry = routingEntry;
        this.trajIds = new ArrayList<>();

        // 检查并创建数据目录
        String dataPath = projectPath + "/output/";
        File dataDir = new File(dataPath);
        if (!dataDir.exists()) {
            if (dataDir.mkdirs()) {
                LOG.info("Created data directory: " + dataPath);
            } else {
                LOG.error("Failed to create data directory: " + dataPath);
                throw new RuntimeException("Failed to create data directory");
            }
        }

        Map<String, Object> conf = new HashMap<>();
        conf.put(DaemonConfig.STORM_METRIC_STORE_CLASS, "org.example.trajstore.rocksdb.RocksDbStore");
        conf.put(DaemonConfig.STORM_ROCKSDB_LOCATION, dataPath + "oldenburg_data_" + prefix);
        conf.put(DaemonConfig.STORM_ROCKSDB_CREATE_IF_MISSING, true);
        conf.put(DaemonConfig.STORM_ROCKSDB_METADATA_STRING_CACHE_CAPACITY, 4000);
        conf.put(DaemonConfig.STORM_ROCKSDB_METRIC_RETENTION_HOURS, 240);
        try {
            store = TrajStoreConfig.configure(conf);
        } catch (TrajStoreException e) {
            LOG.error("Failed to configure TrajStore", e);
            // throw new RuntimeException("Failed to configure TrajStore", e);
        }
    }

    // key1 比 key2
    public Integer calDistanceInKey(String key1, String key2) {
        int m = key1.length();
        int n = key2.length();
        int[][] dp = new int[m + 1][n + 1];

        // 初始化第一行和第一列
        for (int i = 0; i <= m; i++) {
            dp[i][0] = i;
        }
        for (int j = 0; j <= n; j++) {
            dp[0][j] = j;
        }

        // 填充DP表
        for (int i = 1; i <= m; i++) {
            for (int j = 1; j <= n; j++) {
                if (key1.charAt(i - 1) == key2.charAt(j - 1)) {
                    dp[i][j] = dp[i - 1][j - 1];
                } else {
                    dp[i][j] = 1 + Math.min(
                        dp[i - 1][j - 1],  // 替换
                        Math.min(
                            dp[i - 1][j],   // 删除
                            dp[i][j - 1]    // 插入
                        )
                    );
                }
            }
        }

        return dp[m][n];
    }

        // 返回closest key以及对应的docker id
    public String[] findKey(String key){
        String[] res = new String[2]; res[0] = ""; res[1] = "";
        if(key.equals(this.prefix)){
            return res;
        }
        String closestKey = NodesService.longestPrefixMatch(key, 
        routingEntry.keySet().toArray(new String[0]));
        
        // Get the corresponding docker name for the closest key
        if (closestKey != null) {
            res[0] = closestKey;
            res[1] = routingEntry.get(closestKey);
        }

        return res;
    }

    // 返回closest key以及对应的docker id
    public String[] insert(String key, List<TrajPoint> trajectory){
        String[] res = new String[2]; res[0] = ""; res[1] = "";

        Integer trajId = trajectory.get(0).getTrajId();
        if(key.equals(this.prefix)){
            doStore(trajectory);
            this.trajIds.add(trajId);
            return res;
        }
        String closestKey = null;
        Integer minDistance = Integer.MAX_VALUE;
        
        // Find the closest key in routing entries
        // for (String routingKey : routingEntry.keySet()) {
        //     Integer distance = calDistanceInKey(key, routingKey);
        //     if (distance < minDistance) {
        //         minDistance = distance;
        //         closestKey = routingKey;
        //     }
        // }
        closestKey = NodesService.longestPrefixMatch(key, 
        routingEntry.keySet().toArray(new String[0]));
        
        // Get the corresponding docker name for the closest key
        if (closestKey != null) {
            res[0] = closestKey;
            res[1] = routingEntry.get(closestKey);
            // You might want to forward the trajectory to the target node here
            LOG.info("Forwarding trajectory " + trajId + " to node: " + res[1]);
        }

        return res;
    }

    private void doStore(List<TrajPoint> trajectory) {
        // LOG.info(this.dockerName + ", size: " + trajectory.size() + "insert into storage.");
        // LOG.info(this.dockerName + ", trajectory id: " + trajectory.get(0).getTrajId());
        // synchronized (store) {
        //     for (TrajPoint point : trajectory) {
        //         try {
        //             store.insert(point);
        //         } catch (TrajStoreException e) {
        //             e.printStackTrace();
        //         }
        //     }
        // }
    }

    public List<TrajPoint> doRead(Integer trajId, long startTime, long endTime, 
            double minLat, double maxLat, double minLng, double maxLng) {
        List<TrajPoint> trajPoints = new ArrayList<>();
        List<Integer> idToQuery;
        if(trajId != -1)idToQuery = List.of(trajId);
        else idToQuery = trajIds;

        // for (Integer id : idToQuery) {
        //     try {
        //         LOG.info("Read =>" + this.dockerName + ": " + id);
        //         List<TrajPoint> trajectoryPoints = rocksDbRead(id, startTime, endTime);

        //         for (TrajPoint point : trajectoryPoints) {
        //             if (point.getOriLat() >= minLat && point.getOriLat() <= maxLat &&
        //                 point.getOriLng() >= minLng && point.getOriLng() <= maxLng &&
        //                 point.getTimestamp() >= startTime && point.getTimestamp() <= endTime) {
        //                 trajPoints.add(point);
        //             }
        //         }
        //     } catch (TrajStoreException e) {
        //         LOG.error("Error reading local trajectory data for ID: " + id, e);
        //     }
        // }
        return trajPoints; // Add this return statement
    }

    private List<TrajPoint> rocksDbRead(Integer id, long startTime, long endTime) throws TrajStoreException{
        FilterOptions filter = new FilterOptions();
        List<TrajPoint> list = new ArrayList<>();
        if (id != -1) {
            filter.setTrajectoryId(id);
        }
        if (startTime != -1) {
            filter.setStartTime(startTime);
        }
        if (endTime != -1) {
            filter.setEndTime(endTime);
        }
        store.scan(filter, list::add);
        if (list.isEmpty()) {
            LOG.info("There is no trajectory {}.", id);
        }
        else{
            LOG.info("read trajectory size: {}. ", list.size());
        }
        return list;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getDockerName() {
        return dockerName;
    }

    public Map<String, String> getRoutingEntry() {
        return routingEntry;
    }
}
