package company.vk.edu.distrib.compute.nihuaway00;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;
import company.vk.edu.distrib.compute.nihuaway00.proto.ReactorKVServiceGrpc;
import company.vk.edu.distrib.compute.nihuaway00.sharding.*;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class NihuawayKVClusterFactory extends KVClusterFactory {
    private static final Logger log = LoggerFactory.getLogger(NihuawayKVClusterFactory.class);

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        Map<String, NodeInfo> nodes = new ConcurrentHashMap<>();
        Map<String, ReactorKVServiceGrpc.ReactorKVServiceStub> stubs = new ConcurrentHashMap<>();

        ports.forEach(port -> {
            String endpoint = "http://localhost:" + (port);
            String grpcEndpoint = "localhost:" + (port + 1);
            ManagedChannel channel = Grpc.newChannelBuilder(grpcEndpoint, InsecureChannelCredentials.create()).build();
            ReactorKVServiceGrpc.ReactorKVServiceStub stub = ReactorKVServiceGrpc.newReactorStub(channel);
            stubs.put(grpcEndpoint, stub);
            nodes.put(endpoint, new NodeInfo(endpoint, grpcEndpoint));
        });

        String shardingStrategyProp = Config.strategy();
        if (log.isInfoEnabled()) {
            log.info("Using strategy: {}", shardingStrategyProp);
        }
        ShardingStrategy strategy = "rendezvous".equals(shardingStrategyProp)
                ? new RendezvousHashingStrategy(nodes)
                : new ConsistentHashingStrategy(nodes, 50);

        int replicaCountProps = Config.replicas();

        NihuawayKVServiceFactory serviceFactory = new NihuawayKVServiceFactory(strategy, replicaCountProps, stubs);

        return new NihuawayKVCluster(strategy, serviceFactory);
    }
}
