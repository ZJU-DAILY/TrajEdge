package org.example.dht;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

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
        for(int i = startPort; i < startPort + nodeNum; ++i){
            // create node
            m_node = new Node(Helper.createSocketAddress(local_ip+":"+i));
            if(i == startPort){
                // first node
                m_contact = m_node.getAddress();
            }
            else{
                // join
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
        }

    }
}
