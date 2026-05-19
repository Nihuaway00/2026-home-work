package company.vk.edu.distrib.compute.nihuaway00.transport.kafka;

import company.vk.edu.distrib.compute.AuditEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaEventProducer {
    private final String TOPIC = "audit";
    private final KafkaProducer<String, String> producer;

    public KafkaEventProducer(Properties props) {
        this.producer = new KafkaProducer<>(props);
    }

    public Future<RecordMetadata> send(AuditEvent event) {
        String json = String.format(
                "{\"method\":\"%s\",\"id\":\"%s\",\"timestamp\":%d}",
                event.method(), event.id(), event.timestamp()
        );

        return producer.send(new ProducerRecord<>(TOPIC, json));
    }

    public void close(){
        producer.close();
    }
}
