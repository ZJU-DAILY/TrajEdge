package org.example;

import org.example.struct.Node;
import io.grpc.Server;
import io.grpc.ServerBuilder;

public class NodeLauncher {
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: NodeLauncher <port>");
            System.exit(1);
        }

        int port = Integer.parseInt(args[0]);
        String nodeId = String.valueOf(port);
        String prefix = "node" + nodeId + "_";

        Node node = new Node(nodeId, prefix);
        Server server = ServerBuilder.forPort(port)
                .addService(node)
                .build()
                .start();

        System.out.println("Node server started, listening on port " + port);
        server.awaitTermination();
    }
}
