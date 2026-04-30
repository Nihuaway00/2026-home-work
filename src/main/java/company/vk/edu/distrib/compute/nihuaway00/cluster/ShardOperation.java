package company.vk.edu.distrib.compute.nihuaway00.cluster;

import company.vk.edu.distrib.compute.nihuaway00.proto.ReactorKVServiceGrpc;

public interface ShardOperation<T> {
    T execute(ReactorKVServiceGrpc.ReactorKVServiceStub stub);
}
