package company.vk.edu.distrib.compute.nihuaway00.app;

import company.vk.edu.distrib.compute.nihuaway00.replication.ReplicaManager;

import java.util.NoSuchElementException;

public class KVCommandService {
    public final ReplicaManager replicaManager;

    public KVCommandService(ReplicaManager replicaManager) {
        this.replicaManager = replicaManager;
    }

    public byte[] handleGetEntity(String id, int ack) {
        byte[] data = replicaManager.get(id, ack);

        if (data == null) {
            throw new NoSuchElementException();
        }

        return data;
    }

    public void handlePutEntity(String id, byte[] value, int ack) {
        replicaManager.put(id, value, ack);
    }

    public void handleDeleteEntity(String id, int ack) {
        replicaManager.delete(id, ack);
    }
}