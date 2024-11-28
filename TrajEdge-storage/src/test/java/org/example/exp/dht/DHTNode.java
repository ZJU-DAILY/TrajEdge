package org.example.exp.dht;

import java.util.*;

public class DHTNode {
    private String nodeId;
    private List<Object[]> data;  // Each element is [key, metadata]
    private int hitCount;
    private List<DHTNode> fingerTable;
    private String type;  // Only used in GeoTrie: "external"/"internal"/"leaf"
    private Map<String, DHTNode> children;  // Only used in GeoTrie

    public DHTNode(String nodeId) {
        this.nodeId = nodeId;
        this.data = new ArrayList<>();
        this.hitCount = 0;
        this.fingerTable = new ArrayList<>();
        this.type = "external";
        this.children = new HashMap<>();
    }

    // Getters and setters
    public String getNodeId() { return nodeId; }
    public List<Object[]> getData() { return data; }
    public int getHitCount() { return hitCount; }
    public void incrementHitCount() { this.hitCount++; }
    public List<DHTNode> getFingerTable() { return fingerTable; }
    public void setFingerTable(List<DHTNode> fingerTable) { this.fingerTable = fingerTable; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public Map<String, DHTNode> getChildren() { return children; }
} 