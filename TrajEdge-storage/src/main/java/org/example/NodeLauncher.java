package org.example;
import io.grpc.Server;
import io.grpc.ServerBuilder;

public class NodeLauncher {
    public static void main(String[] args) throws Exception {
        int port = args.length != 1 ? 9999 : Integer.parseInt(args[0]);
        String nodeId = System.getenv("CONTAINER_ID");

        NodesService ns = new NodesService(nodeId, port);
        Server server = ServerBuilder.forPort(port)
                .addService(ns)
                .build()
                .start();

        System.out.println("Node server started, listening on port " + port);
        server.awaitTermination();
    }
}
