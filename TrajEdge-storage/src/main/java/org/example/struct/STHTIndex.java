package org.example.struct;

import org.example.coding.CodingRange;
import org.example.coding.XZ2Coding;
import org.example.coding.XZTCoding;
import org.example.datatypes.TimeLine;
import org.example.trajstore.TrajPoint;
import org.example.struct.TrieNode;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.example.datatypes.ByteArray;
import java.time.Instant;

public class STHTIndex {
    private static final Logger LOG = LoggerFactory.getLogger(STHTIndex.class);
    // 常量定义
    private static final double X_MIN = -90.0;
    private static final double X_MAX = 90.0;
    private static final double Y_MIN = -180.0;
    private static final double Y_MAX = 180.0;
    private static final Integer ringSize = 3;
    public static double LogPointFive = Math.log(0.5);
    private static String dockerName;


    private XZTCoding xztCoding;
    private XZ2Coding xz2Coding;
    private TrieNode root;
    protected double xSize, ySize;
    private Long maxBinNum, minBinNum;

    public STHTIndex() {
        this.root = new TrieNode();
        xztCoding = new XZTCoding();
        xz2Coding = new XZ2Coding();
        maxBinNum = 0L;
        minBinNum = Long.MAX_VALUE;
        dockerName = System.getenv("CONTAINER_ID");
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

            if(!range.getLower().equals(range.getUpper())){
                res = new String[2];
                array = range.getUpper();
                res[0] = String.valueOf(xztCoding.getBinNum(array));
                res[1] = xztCoding.getRawCode(array);
                st.add(res);
            }
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

    public String insertTrajectory(List<TrajPoint> trajectory) {
        if (trajectory == null || trajectory.isEmpty()) {
            return "";
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

        LOG.info("insert into trie: " + timeCode[0] + "_" + combinedCode);
        return insertIntoTrie(timeCode[0], combinedCode, trajectory.get(0).getTrajId());
    }

    private String insertIntoTrie(String binNum, String combinedCode, int trajId) {
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
                String nextNode = decideNode(binNum);
                current.remoteNode = new Node(nextNode);
                root.children.add(current);
                return nextNode;
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
        maxBinNum = Math.max(maxBinNum, Integer.parseInt(binNum));
        minBinNum = Math.min(minBinNum, Integer.parseInt(binNum));
        return "";
    }

    private String decideNode(String binNum){
        String no = dockerName.substring("supervisor-".length());
        if(no.isEmpty()){
           throw new RuntimeException("Can't get env of supervisor: " + dockerName);
        }
        Integer num = Integer.parseInt(no);
        num += ringSize;
        Integer nextNum;

        if((num % ringSize) < minBinNum)nextNum = (num - 1) % ringSize;
        else nextNum = (num + 1) % ringSize;
        if(nextNum == 0)nextNum = ringSize;

        return "supervisor-" + nextNum;
    }

    public List<Integer> query(long startTime, long endTime, double minLat, double maxLat, double minLng, double maxLng, Set<Node> remoteNodes) {
        List<Integer> result = new ArrayList<>();
        List<String> spatialCode = generateSpacialKeyRanges(minLat, maxLat, minLng, maxLng);
        List<String[]> temporalCode = generateTemporalKeyRanges(startTime, endTime);
        for(String spatial : spatialCode){
            for(String[] temporal : temporalCode){
                String combined = concatTimeAndSpace(temporal[1],spatial);
                LOG.info("Query code: " + temporal[0] + "_" + combined);
                result.addAll(queryTrie(temporal[0], combined, remoteNodes));
            }
        }

        return result;
    }

    private List<Integer> queryTrie(String binNum, String combinedCode, Set<Node> remoteNodes) {
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
            res.append(timeCode.charAt(i));
            res.append(spaceCode.charAt(i));
        }
        return res.toString();
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
}