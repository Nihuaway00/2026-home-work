package company.vk.edu.distrib.compute.nihuaway00.sharding;

import company.vk.edu.distrib.compute.nihuaway00.proto.ReactorKVServiceGrpc;

public interface ShardOperation<T> {
    T execute(ReactorKVServiceGrpc.ReactorKVServiceStub stub);
}
