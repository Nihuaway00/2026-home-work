package company.vk.edu.distrib.compute.nihuaway00.audit;

import company.vk.edu.distrib.compute.AuditEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

public class AuditService implements company.vk.edu.distrib.compute.AuditService {
    private final Logger log = LoggerFactory.getLogger(AuditService.class);
    private static final String TOPIC = "audit";
    private final String bootstrapServers;
    private final String groupId;
    private final Path storageFile;
    private final List<AuditEvent> events = new CopyOnWriteArrayList<>();

    @SuppressWarnings("PMD.AvoidUsingVolatile")
    private volatile boolean running;
    private KafkaConsumer<String, String> consumer;
    private ExecutorService executor;

    public AuditService(String bootstrapServers, String groupId) {
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
        this.storageFile = Path.of("audit-" + groupId + ".log");
    }

    @Override
    public void start() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // будет читать с конца топика при первом подключении
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // авто сохранение оффсета
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        // счетчик, чтобы дождаться момента, когда consumer подключиться к партиции
        CountDownLatch assignedLatch = new CountDownLatch(1);

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(TOPIC), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // отзыв партиции не требует действий
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                // консьюмеру выдана партиция, значит можно продолжать start()
                assignedLatch.countDown();
            }
        });
        running = true;

        executor = Executors.newSingleThreadExecutor();
        executor.submit(this::pollLoop);

        try {
            assignedLatch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("AuditService startup interrupted", e);
        }
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

    @SuppressWarnings("PMD.UseTryWithResources")
    private void pollLoop() {
        // stop() вызывает consumer.wakeup() из другого потока, что останавливает консьюмера;
        // try-with-resources здесь неприменим без разрушения этой межпоточной зависимости
        try {
            while (running) {
                var records = consumer.poll(Duration.ofMillis(200));
                for (var record : records) {
                    AuditEvent event = parse(record.value());
                    if (event != null) {
                        appendToFile(record.value());
                        events.add(event);
                    }
                }
            }
        } catch (WakeupException e) {
            if (log.isDebugEnabled()) {
                log.debug("Attempt to stop Audit Service with stop() command");
            }
        } finally {
            consumer.close();
        }
    }

    @Override
    public List<AuditEvent> listAuditEntries() {
        return List.copyOf(events);
    }

    private void appendToFile(String rawJson) {
        try {
            Files.writeString(storageFile, rawJson + "\n",
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
