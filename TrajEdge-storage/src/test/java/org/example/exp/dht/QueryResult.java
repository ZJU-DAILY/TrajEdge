package org.example.exp.dht;

import java.util.List;

public class QueryResult<T> {
    private final List<T> results;
    private final int routingLength;
    private final double hitRatio;
    private final List<DHTNode> path;
    
    public QueryResult(List<T> results, int routingLength, double hitRatio, List<DHTNode> path) {
        this.results = results;
        this.routingLength = routingLength;
        this.hitRatio = hitRatio;
        this.path = path;
    }
    
    public List<T> getResults() { return results; }
    public int getRoutingLength() { return routingLength; }
    public double getHitRatio() { return hitRatio; }
    public List<DHTNode> getPath() { return path; }
} 