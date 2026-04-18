package company.vk.edu.distrib.compute.nihuaway00.sharding;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Stream;

public class ConsistentHashingStrategy implements ShardingStrategy {
    private final NavigableMap<Long, NodeInfo> ring = new TreeMap<>();
    private final int virtualNodes;

    public ConsistentHashingStrategy(Map<String, NodeInfo> nodes, int virtualNodes) {
        this.virtualNodes = virtualNodes;
        nodes.values().forEach(this::addNode);
    }

    private void addNode(NodeInfo node) {
        for (int i = 0; i < virtualNodes; i++) {
            long hash = computeHash(node.getEndpoint() + "#" + i);
            ring.put(hash, node);
        }
    }

    @Override
    public NodeInfo getResponsibleNode(String key) {
        long hash = computeHash(key);
        SortedMap<Long, NodeInfo> tail = ring.tailMap(hash);
        return Stream.concat(tail.values().stream(), ring.values().stream())
                .filter(NodeInfo::isEnabled)
                .findFirst()
                .orElseThrow(() -> new NoSuchElementException("No alive nodes"));
    }

    @Override
    public void enableNode(String endpoint) {
        ring.values().stream()
                .filter(n -> n.getEndpoint().equals(endpoint))
                .forEach(NodeInfo::enable);
    }

    @Override
    public void disableNode(String endpoint) {
        ring.values().stream()
                .filter(n -> n.getEndpoint().equals(endpoint))
                .forEach(NodeInfo::disable);
    }

    @Override
    public List<String> getEndpoints() {
        return ring.values().stream()
                .map(NodeInfo::getEndpoint)
                .distinct()
                .toList();
    }

    private long computeHash(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] hash = md.digest(key.getBytes(StandardCharsets.UTF_8));
            return ByteBuffer.wrap(hash).getLong();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e); // MD5 всегда есть в JVM
        }
    }
}
