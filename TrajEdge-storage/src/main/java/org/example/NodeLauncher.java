package org.example;

import org.example.struct.Node;
import io.grpc.Server;
import io.grpc.ServerBuilder;

public class NodeLauncher {
    public static void main(String[] args) throws Exception {
        int port = args.length != 1 ? 9999 : Integer.parseInt(args[0]);
        String nodeId = String.valueOf(port);

        Node node = new Node(nodeId, port);
        Server server = ServerBuilder.forPort(port)
                .addService(node)
                .build()
                .start();

        System.out.println("Node server started, listening on port " + port);
        server.awaitTermination();
    }
}
