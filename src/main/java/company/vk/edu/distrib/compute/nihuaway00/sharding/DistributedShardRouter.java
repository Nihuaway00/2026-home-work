package company.vk.edu.distrib.compute.nihuaway00.sharding;

import company.vk.edu.distrib.compute.nihuaway00.proto.ReactorKVServiceGrpc;

import java.net.URI;
import java.util.Map;

public class DistributedShardRouter implements ShardRouter {
    private final String currentNodeEndpoint;
    private final String currentNodeGrpcEndpoint;
    private final ShardingStrategy shardingStrategy;
    private final Map<String, ReactorKVServiceGrpc.ReactorKVServiceStub> stubs;

    public DistributedShardRouter(String currentNodeEndpoint, ShardingStrategy strategy, Map<String, ReactorKVServiceGrpc.ReactorKVServiceStub> stubs) {
        this.currentNodeEndpoint = currentNodeEndpoint;
        this.currentNodeGrpcEndpoint = toGrpcEndpoint(currentNodeEndpoint);
        this.shardingStrategy = strategy;
        this.stubs = stubs;
    }

    @Override
    public String getResponsibleNode(String key) {
        return shardingStrategy.getResponsibleNode(key).getGrpcEndpoint();
    }

    @Override
    public <T> T proxyRequest(String shardId, ShardOperation<T> operation) {
        return operation.execute(stubs.get(shardId));
    }

    @Override
    public boolean isLocalNode(String endpoint) {
        return currentNodeEndpoint.equals(endpoint) || currentNodeGrpcEndpoint.equals(endpoint);
    }

    private static String toGrpcEndpoint(String endpoint) {
        URI uri = URI.create(endpoint);
        return uri.getHost() + ":" + (uri.getPort() + 1);
    }
}
