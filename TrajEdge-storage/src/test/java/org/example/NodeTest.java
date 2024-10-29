package org.example;
import org.example.struct.Node;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

public class NodeTest {

    @Test
    public void testCalDistanceInKey() {
        Node node = new Node("testPrefix"); // 创建 Node 实例

        // 测试用例
        assertTrue(1 == node.calDistanceInKey("#31", "#301")); // 相同字符串
        assertTrue(2 == node.calDistanceInKey("#31", "#300")); // 相同字符串
        // assertTrue(1 == node.calDistanceInKey("test", "tes"));  // 删除一个字符
        // assertTrue(1 == node.calDistanceInKey("tes", "test"));  // 插入一个字符
        // assertTrue(2 == node.calDistanceInKey("test", "tast"));  // 替换一个字符
        // assertTrue(4 == node.calDistanceInKey("test", "tasty")); // 不同字符串
    }
}