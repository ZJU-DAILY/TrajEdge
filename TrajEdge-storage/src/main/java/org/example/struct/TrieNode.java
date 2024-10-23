package org.example.struct;
import java.util.List;
import java.util.ArrayList;

public class TrieNode {
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