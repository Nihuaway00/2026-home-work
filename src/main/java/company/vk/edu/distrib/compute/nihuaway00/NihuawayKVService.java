package company.vk.edu.distrib.compute.nihuaway00;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.nihuaway00.app.KVCommandService;
import company.vk.edu.distrib.compute.nihuaway00.http.EntityHandler;
import company.vk.edu.distrib.compute.nihuaway00.http.PingHandler;
import company.vk.edu.distrib.compute.nihuaway00.replication.ReplicaManager;
import company.vk.edu.distrib.compute.nihuaway00.sharding.ShardRouter;
import company.vk.edu.distrib.compute.nihuaway00.transport.grpc.InternalGrpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class NihuawayKVService implements company.vk.edu.distrib.compute.ReplicatedService {
    private static final Logger log = LoggerFactory.getLogger(NihuawayKVService.class);

    private final ReplicaManager replicaManager;
    private HttpServer server;
    private InternalGrpcService grpcServer;
    private final ShardRouter shardRouter;
    int port;

    NihuawayKVService(int port, ShardRouter shardRouter, ReplicaManager replicaManager) {
        this.port = port;
        this.shardRouter = shardRouter;
        this.replicaManager = replicaManager;

    }

    @Override
    public void start() {
        try {
            InetSocketAddress addr = new InetSocketAddress(port);
            server = HttpServer.create(addr, 0);
            server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
            registerContexts();
            server.start();
            if (grpcServer.isShutdown() || grpcServer.isTerminated()) {
                grpcServer.newGrpcServer(port + 1);
            }
            grpcServer.start();
        } catch (Exception e) {
            if (log.isErrorEnabled()) {
                log.error(e.getMessage());
            }
        }
    }

    @Override
    public void stop() {
        if (grpcServer != null) {
            grpcServer.shutdown();
            grpcServer = null;
        }

        if (server != null) {
            server.stop(0);
            server = null;
        } else {
            if (log.isWarnEnabled()) {
                log.warn("Server is not started");
            }
        }
    }

    private void registerContexts() throws IOException {
        KVCommandService commandService = new KVCommandService(replicaManager);
        grpcServer = new InternalGrpcService(port, commandService);
        server.createContext("/v0/entity", new EntityHandler(commandService));
        server.createContext("/v0/status", new PingHandler(replicaManager));
    }

    @Override
    public int port() {
        return port;
    }

    @Override
    public int numberOfReplicas() {
        return replicaManager.numberOfReplicas();
    }

    @Override
    public void disableReplica(int nodeId) {
        replicaManager.disableReplica(nodeId);
    }

    @Override
    public void enableReplica(int nodeId) {
        replicaManager.enableReplica(nodeId);
    }
}
