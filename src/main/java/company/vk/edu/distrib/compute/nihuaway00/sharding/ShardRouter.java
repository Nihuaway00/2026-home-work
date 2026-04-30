package company.vk.edu.distrib.compute.nihuaway00.sharding;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;

public interface ShardRouter {
    <T> T proxyRequest(String targetNodeEndpoint, ShardOperation<T> operation);

    String getResponsibleNode(String key);

    boolean isLocalNode(String endpoint);
}
