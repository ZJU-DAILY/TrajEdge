package org.example.dht;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author alecHe
 * @desc ...
 * @date 2024-01-10 14:25:07
 */
public class StoreCluster {
    private static Node m_node;
    private static InetSocketAddress m_contact;
    private static Helper m_helper;

    public static void main(String[] args) {
        int nodeNum = 10;
        int startPort = 10100;
        m_helper = new Helper();
        // get local machine's ip
        String local_ip = null;
        try {
            local_ip = InetAddress.getLocalHost().getHostAddress();

        } catch (UnknownHostException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        System.out.println("cluster starting at " + local_ip);
        // create node
        Map<String, String> conf = new HashMap<>();
        if (args.length > 0) {
            conf.put("data.dest", "/home/hch/PROJECT/storm/outputData/RocksDbStoreTest/");
        }
        else{
            conf.put("data.dest", "F:/RocksDbStoreTest/");
        }

        for(int i = startPort; i < startPort + nodeNum; ++i){
            // create node
            m_node = new Node(Helper.createSocketAddress(local_ip+":"+i), conf);
            if(i == startPort){
                // first node
                m_contact = m_node.getAddress();
            }
            else{
                // join
                m_contact = Helper.createSocketAddress(local_ip+":"+startPort);
                if (m_contact == null) {
                    System.out.println("Cannot find address you are trying to contact. Now exit.");
                    return;
                }

            }
            // try to join ring from contact node
            boolean successful_join = m_node.join(m_contact);

            // fail to join contact node
            if (!successful_join) {
                System.out.println("Cannot connect with node you are trying to contact. Now exit.");
                System.exit(0);
            }

            // print join info
            System.out.println("Joining the Chord ring.");
            System.out.println("Local IP: "+local_ip);
            m_node.printNeighbors();
        }
        System.out.println("all node is set.");
    }
}
