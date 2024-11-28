package org.example.exp.dht;

import java.util.List;

public class DHTRoutingResult {
    private final DHTNode node;
    private final List<DHTNode> path;
    
    public DHTRoutingResult(DHTNode node, List<DHTNode> path) {
        this.node = node;
        this.path = path;
    }
    
    public DHTNode getNode() { return node; }
    public List<DHTNode> getPath() { return path; }
} 