package company.vk.edu.distrib.compute.nihuaway00.audit;

import company.vk.edu.distrib.compute.AuditEvent;
import company.vk.edu.distrib.compute.nihuaway00.transport.kafka.KafkaEventProducer;

public class AsyncAuditSender implements AuditSender {
    private final KafkaEventProducer producer;

    public AsyncAuditSender(KafkaEventProducer producer) {
        this.producer = producer;
    }

    @Override
    public void send(AuditEvent event) {
        producer.send(event);
    }
}
