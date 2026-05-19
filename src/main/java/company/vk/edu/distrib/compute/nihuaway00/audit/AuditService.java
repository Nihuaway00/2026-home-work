package company.vk.edu.distrib.compute.nihuaway00.audit;

import company.vk.edu.distrib.compute.AuditEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AuditService implements company.vk.edu.distrib.compute.AuditService {
    private static final String TOPIC = "audit";

    private final String bootstrapServers;
    private final String groupId;
    private final List<AuditEvent> events = new CopyOnWriteArrayList<>();

    private volatile boolean running;
    private KafkaConsumer<String, String> consumer;
    private ExecutorService executor;

    public AuditService(String bootstrapServers, String groupId) {
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
    }

    @Override
    public void start() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); // будет читать с конца топика при первом подключении
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"); // авто сохранение оффсета

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(TOPIC));
        running = true;

        executor = Executors.newSingleThreadExecutor();
        executor.submit(this::pollLoop);
    }

    @Override
    public void stop() {
        running = false;
        if (consumer != null) {
            consumer.wakeup();
        }
        if (executor != null) {
            executor.shutdown();
            try {
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void pollLoop() {
        try {
            while (running) {
                var records = consumer.poll(Duration.ofMillis(200));
                for (var record : records) {
                    AuditEvent event = parse(record.value());
                    if (event != null) {
                        events.add(event);
                    }
                }
            }
        } catch (WakeupException e) {
            // нормальное завершение через stop()
        } finally {
            consumer.close();
        }
    }

    @Override
    public List<AuditEvent> listAuditEntries() {
        return List.copyOf(events);
    }

    // {"method":"PUT","id":"someId","timestamp":123}
    private static AuditEvent parse(String json) {
        try {
            String method = extractString(json, "method");
            String id = extractString(json, "id");
            long timestamp = extractLong(json, "timestamp");
            return new AuditEvent(method, id, timestamp);
        } catch (Exception e) {
            return null;
        }
    }

    private static String extractString(String json, String key) {
        String search = "\"" + key + "\":\"";
        int start = json.indexOf(search) + search.length();
        int end = json.indexOf('"', start);
        return json.substring(start, end);
    }

    private static long extractLong(String json, String key) {
        String search = "\"" + key + "\":";
        int start = json.indexOf(search) + search.length();
        int end = start;
        while (end < json.length() && (Character.isDigit(json.charAt(end)) || json.charAt(end) == '-')) {
            end++;
        }
        return Long.parseLong(json.substring(start, end));
    }
}
