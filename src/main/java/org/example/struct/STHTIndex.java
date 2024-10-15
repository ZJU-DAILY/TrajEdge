package org.example.struct;

import org.apache.hadoop.hive.ql.parse.HiveParser.nullOrdering_return;
import org.example.coding.CodingRange;
import org.example.coding.XZ2Coding;
import org.example.coding.XZTCoding;
import org.example.coding.sfc.TimeIndexRange;
import org.example.datatypes.TimeLine;
import org.example.trajstore.TrajPoint;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import clojure.lang.IFn.LO;

import org.example.datatypes.ByteArray;

import java.util.LinkedList;
import java.time.Instant;

public class STHTIndex {
    private static final Logger LOG = LoggerFactory.getLogger(STHTIndex.class);
    private static final long REF_TIME = 0; // 1970-01-01T00:00:00Z
    private static final int BIN_LEN = 86400; // 一天的秒数
    private static final int BETA = 2; // 分区大小
    private static final int g = 3; // 编码位宽
    // 常量定义
    private static final double X_MIN = -90.0;
    private static final double X_MAX = 90.0;
    private static final double Y_MIN = -180.0;
    private static final double Y_MAX = 180.0;
    protected double xSize, ySize;
    static double LogPointFive = Math.log(0.5);

    private XZTCoding xztCoding;
    private XZ2Coding xz2Coding;
    private TrieNode root;

    public STHTIndex() {
        this.root = new TrieNode();
        xztCoding = new XZTCoding();
        xz2Coding = new XZ2Coding();
    }

    public String[] encodeTime(long startTime, long endTime) {
        ZonedDateTime stTime = Instant.ofEpochMilli(startTime).atZone(ZoneId.systemDefault());
        ZonedDateTime enTime = Instant.ofEpochMilli(endTime).atZone(ZoneId.systemDefault());
        TimeLine timeLine = new TimeLine(stTime, enTime);
        String[] res = new String[2];
        ByteArray array = xztCoding.index(timeLine);
        res[0] = String.valueOf(xztCoding.getBinNum(array));
        res[1] = xztCoding.getRawCode(array);

        return res;
    }


    public String encodeSpatial(double minLat, double maxLat, double minLng, double maxLng) {
        return xz2Coding.getRawCode(xz2Coding.rawCode(minLng, maxLng, minLat, maxLat));
    }

    private List<String[]> generateTemporalKeyRanges(long startTime, long endTime){
        ZonedDateTime stTime = Instant.ofEpochMilli(startTime).atZone(ZoneId.systemDefault());
        ZonedDateTime enTime = Instant.ofEpochMilli(endTime).atZone(ZoneId.systemDefault());
        TimeLine timeLine = new TimeLine(stTime, enTime);
        List<CodingRange> ranges = xztCoding.rawRangesAfterCoding(List.of(timeLine));

        Set<String[]> st = new HashSet<>();

        for(CodingRange range : ranges){
            String[] res = new String[2];
            ByteArray array = range.getLower();
            res[0] = String.valueOf(xztCoding.getBinNum(array));
            res[1] = xztCoding.getRawCode(array);
            st.add(res);

            res = new String[2];
            array = range.getUpper();
            res[0] = String.valueOf(xztCoding.getBinNum(array));
            res[1] = xztCoding.getRawCode(array);
            st.add(res);
        }
        return new ArrayList<>(st);
    }

    // lng是x
    private List<String> generateSpacialKeyRanges(double minLat, double maxLat, double minLng, double maxLng){
        List<CodingRange> ranges = xz2Coding.rawRanges(minLng, minLat, maxLng, maxLat);
        Set<String> st = new HashSet<>();

        for(CodingRange range : ranges){
            st.add(xz2Coding.getRawCode(range.getLower()));
            st.add(xz2Coding.getRawCode(range.getUpper()));
        }
        return new ArrayList<>(st);
    }

    public boolean insertTrajectory(List<TrajPoint> trajectory) {
        if (trajectory == null || trajectory.isEmpty()) {
            return true;
        }

        long startTime = trajectory.get(0).getTimestamp();
        long endTime = trajectory.get(trajectory.size() - 1).getTimestamp();
        double minLat = X_MAX, maxLat = X_MIN;
        double minLng = Y_MAX, maxLng = Y_MIN;

        for (TrajPoint point : trajectory) {
            minLat = Math.min(minLat, point.getOriLat());
            maxLat = Math.max(maxLat, point.getOriLat());
            minLng = Math.min(minLng, point.getOriLng());
            maxLng = Math.max(maxLng, point.getOriLng());
        }

        String[] timeCode = encodeTime(startTime, endTime);
        String spatialCode = encodeSpatial(minLat, maxLat, minLng, maxLng);
        String combinedCode = concatTimeAndSpace(timeCode[1], spatialCode);

        LOG.info("insert into trie: " + timeCode[0] + "_" + timeCode[1] + "_" + spatialCode);
        return insertIntoTrie(timeCode[0], combinedCode, trajectory.get(0).getTrajId());
    }

    private boolean insertIntoTrie(String binNum, String combinedCode, int trajId) {
        TrieNode current = root;
        for(TrieNode child : root.children){
            if(child.curValue.equals(binNum)){
                current = child;
                break;
            }
        }
        if(current == root){
            if(root.children.size() > 2){
                current = new TrieNode(root, root.prefix + root.curValue, binNum, 3);
                current.remoteNode = new Node(decideNode(binNum), "");
                root.children.add(current);
                return false;
            }
            current = new TrieNode(root, root.prefix + root.curValue, binNum, 1);
            root.children.add(current);
        }

        for (int i = 0; i < combinedCode.length(); i+=2) {
            String subCode = combinedCode.substring(i, i + 1);
            TrieNode tmp = current;
            for(TrieNode child : current.children){
                if(child.curValue.equals(subCode)){
                    current = child;
                    break;
                }
            }
            // Not Found
            if(current == tmp){
                current = new TrieNode(tmp, tmp.prefix + tmp.curValue, subCode, 2);
                tmp.children.add(current);
            }
        }
        current.trajIds.add(trajId);
        return true;
    }

    private String decideNode(String binNum){
        return "50052";
    }

    public List<Integer> query(long startTime, long endTime, double minLat, double maxLat, double minLng, double maxLng, List<Node> remoteNodes) {
        List<Integer> result = new ArrayList<>();
        List<String> spatialCode = generateSpacialKeyRanges(minLat, maxLat, minLng, maxLng);
        List<String[]> temporalCode = generateTemporalKeyRanges(startTime, endTime);
        for(String spatial : spatialCode){
            for(String[] temporal : temporalCode){
                String combined = concatTimeAndSpace(temporal[1],spatial);
                LOG.info("Query code: " + temporal[0] + "_" + temporal[1] + "_" + spatial);
                result.addAll(queryTrie(temporal[0], combined, remoteNodes));
            }
        }

        return result;
    }

    private List<Integer> queryTrie(String binNum, String combinedCode, List<Node> remoteNodes) {
        TrieNode current = root;
        List<Integer> result = new ArrayList<>();

        for(TrieNode child : root.children){
            if(child.curValue.equals(binNum)){
                current = child;
                break;
            }
        }
        if(current == root || current.nodeType == 3){
            remoteNodes.add(current.remoteNode);
            return result;
        } 
        
        for (int i = 0; i < combinedCode.length(); i+=2) {
            String subCode = combinedCode.substring(i, i + 1);
            TrieNode tmp = current;
            for(TrieNode child : current.children){
                if(child.curValue.equals(subCode)){
                    current = child;
                    break;
                }
            }
            // Not in Local
            if(current.nodeType == 3){
                remoteNodes.add(current.remoteNode);
                return result;
            }
            // Not Found
            else if(current == tmp){
                return result;
            }
        }
        result = current.trajIds;
        return result;
    }

    private String concatTimeAndSpace(String timeCode, String spaceCode){
        StringBuilder res = new StringBuilder();
        for(int i = 0; i < timeCode.length(); i++){
            res.append(timeCode.charAt(i) + spaceCode.charAt(i));
        }
        return res.toString();
    }

    public static class TrieNode {
        // (0 root, 1 binNum, 2 code) local, 3 remote
        int nodeType;
        String prefix;  // 新增：表示这个节点对应的前缀值
        String curValue;

        TrieNode father;
        List<TrieNode> children;
        List<Integer> trajIds;
        Node remoteNode;

        TrieNode() {
            this.children = new ArrayList<>(); // 二进制，所以只有 0 和 1 两个子节点
            this.trajIds = new ArrayList<>();
            this.prefix = "";  // 初始化为空字符串
            nodeType = 0;
            curValue = "";
        }

        TrieNode(TrieNode father, String prefix, String value, int type) {
            this();
            this.prefix = prefix;
            this.curValue = value;
            this.nodeType = type;
            this.father = father;
        }
    }

    private TrieNode findNodeToEvicted(TrieNode root){
        if(root == null)return null;
        if(root.children.isEmpty() && root.nodeType != 3)return root;
        for(TrieNode node : root.children){
            TrieNode toEvict = findNodeToEvicted(node);
            if(toEvict != null)return toEvict;
        }
        return null;
    }

    public TrieNode naiveFindNodeToEvicted(TrieNode root){
        if(root == null)return null;
        for(TrieNode node : root.children){
            if(node.nodeType != 3)return node;
        }
        return null;
    }

    public void evictNode(TrieNode node){
        node.children.clear();
        node.nodeType = 3;
    }
    
    // 可以添加一个方法来获取特定前缀的TrieNode
    public TrieNode getNodeByPrefix(String prefix) {
        TrieNode current = root;
        // for (char bit : prefix.toCharArray()) {
        //     int index = bit - '0';
        //     if (current.children[index] == null) {
        //         return null;  // 前缀不存在
        //     }
        //     current = current.children[index];
        // }
        return current;
    }

    public List<TrieNode> getPredecessorIds(String prefix) {
        List<TrieNode> predecessors = new ArrayList<>();
        TrieNode node = getNodeByPrefix(prefix);
        if (node == null) {
            return predecessors;
        }

        // // 获取前两个前缀较小的节点
        // TrieNode current = root;
        // for (char bit : prefix.toCharArray()) {
        //     int index = bit - '0';
        //     if (index > 0 && current.children[0] != null) {
        //         predecessors.add(current.children[0]);
        //     }
        //     if (current.children[index] == null) {
        //         break;
        //     }
        //     current = current.children[index];
        // }

        // // 如果还没有找到两个前驱，继续向上查找
        // while (predecessors.size() < 2 && current != root) {
        //     TrieNode parent = findParent(current);
        //     if (parent != null && parent != root) {
        //         for (int i = 0; i < 2; i++) {
        //             if (parent.children[i] != null && parent.children[i] != current) {
        //                 predecessors.add(parent.children[i]);
        //                 break;
        //             }
        //         }
        //     }
        //     current = parent;
        // }

        return predecessors.subList(0, Math.min(2, predecessors.size()));
    }

    public List<TrieNode> getSuccessorIds(String prefix) {
        List<TrieNode> successors = new ArrayList<>();
        TrieNode node = getNodeByPrefix(prefix);
        if (node == null) {
            return successors;
        }

        // // 获取前两个前缀较大的节点
        // for (int i = 0; i < 2; i++) {
        //     if (node.children[i] != null) {
        //         successors.add(node.children[i]);
        //     }
        // }

        // 如果还没有找到两个后继，继续向下查找
        while (successors.size() < 2) {
            TrieNode nextNode = findNextNode(node);
            if (nextNode != null) {
                successors.add(nextNode);
            } else {
                break;
            }
            node = nextNode;
        }

        return successors.subList(0, Math.min(2, successors.size()));
    }

    private TrieNode findParent(TrieNode node) {
        if (node == root) {
            return null;
        }
        String parentPrefix = node.prefix.substring(0, node.prefix.length() - 1);
        return getNodeByPrefix(parentPrefix);
    }

    private TrieNode findNextNode(TrieNode node) {
        // if (node.children[1] != null) {
        //     return node.children[1];
        // }
        // TrieNode parent = findParent(node);
        // while (parent != null) {
        //     if (parent.children[1] != null && parent.children[1] != node) {
        //         return parent.children[1];
        //     }
        //     node = parent;
        //     parent = findParent(node);
        // }
        return null;
    }

    
}