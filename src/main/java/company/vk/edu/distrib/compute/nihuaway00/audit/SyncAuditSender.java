package company.vk.edu.distrib.compute.nihuaway00.audit;

import company.vk.edu.distrib.compute.AuditEvent;
import company.vk.edu.distrib.compute.nihuaway00.transport.kafka.KafkaEventProducer;

import java.util.concurrent.ExecutionException;

public class SyncAuditSender implements AuditSender {
    private final KafkaEventProducer producer;

    public SyncAuditSender(KafkaEventProducer producer) {
        this.producer = producer;
    }

    @Override
    public void send(AuditEvent event) {
        try {
            producer.send(event).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
