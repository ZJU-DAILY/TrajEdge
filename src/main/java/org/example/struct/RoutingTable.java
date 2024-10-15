package org.example.struct;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
public class RoutingTable {
    // Prefix -> NodeID
    private Map<String, String> predecessors;
    private Map<String, String> successors;

    public RoutingTable() {
        this.predecessors = new HashMap<>();
        this.successors = new HashMap<>();
    }

    public void addPredecessors(List<String> trieNodes, String nodeID) {
        for (int i = 0; i < trieNodes.size(); i++) {
            predecessors.put(trieNodes.get(i), nodeID);
        }
    }

    public void addSuccessors(List<String> trieNodes, String nodeID) {
        for (int i = 0; i < trieNodes.size(); i++) {
            successors.put(trieNodes.get(i), nodeID);
        }
    }

    public String getPredecessor(String prefix) {
        return predecessors.get(prefix);
    }

    public String getSuccessor(String prefix) {
        return successors.get(prefix);
    }
}
