package company.vk.edu.distrib.compute.nihuaway00.app;

import company.vk.edu.distrib.compute.nihuaway00.replication.ReplicaManager;
import company.vk.edu.distrib.compute.nihuaway00.cluster.ShardRouter;

import java.util.NoSuchElementException;

public class KVCommandService {
    public final ReplicaManager replicaManager;
    private final ShardRouter shardRouter;
    private final InternalNodeClient internalNodeClient;

    public KVCommandService(ReplicaManager replicaManager, ShardRouter shardRouter, InternalNodeClient internalNodeClient) {
        this.replicaManager = replicaManager;
        this.shardRouter = shardRouter;
        this.internalNodeClient = internalNodeClient;
    }

    public byte[] handleGetEntity(String id, int ack) {
        String target = shardRouter.getResponsibleNode(id);

        if (shardRouter.isLocalNode(target)) {
            byte[] data = replicaManager.get(id, ack);

            if (data == null) {
                throw new NoSuchElementException();
            }

            return data;
        } else {
            return internalNodeClient.get(target, id, ack);
        }
    }

    public void handlePutEntity(String id, byte[] value, int ack) {
        String target = shardRouter.getResponsibleNode(id);
        if (shardRouter.isLocalNode(target)) {
            replicaManager.put(id, value, ack);
            return;
        } else {
            internalNodeClient.put(target, id, value, ack);
            return;
        }
    }

    public void handleDeleteEntity(String id, int ack) {
        String target = shardRouter.getResponsibleNode(id);
        if (shardRouter.isLocalNode(target)) {
            replicaManager.delete(id, ack);
            return;
        } else {
            internalNodeClient.delete(target, id, ack);
            return;
        }
    }
}