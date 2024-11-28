package org.example;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.Map;

public class NodeLauncher {
    public static void main(String[] args) throws Exception {
        int port = args.length != 1 ? 9999 : Integer.parseInt(args[0]);
        String nodeId = System.getenv("CONTAINER_ID");
        String nodeInfoStr = System.getenv("NODE_INFO");
        
        // 将字符串转换回Map
        Type type = new TypeToken<Map<String, Map<String, String>>>(){}.getType();
        Map<String, Map<String, String>> nodeInfo = new Gson().fromJson(nodeInfoStr, type);

        NodesService ns = new NodesService(nodeId, port, false, nodeInfo);
        Server server = ServerBuilder.forPort(port)
                .addService(ns)
                .build()
                .start();

        System.out.println("Node server started, listening on port " + port);
        server.awaitTermination();
    }
}
