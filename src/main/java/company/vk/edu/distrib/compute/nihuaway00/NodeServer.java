package company.vk.edu.distrib.compute.nihuaway00;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.AuditableKVService;
import company.vk.edu.distrib.compute.nihuaway00.app.KVCommandService;
import company.vk.edu.distrib.compute.nihuaway00.audit.AsyncAuditSender;
import company.vk.edu.distrib.compute.nihuaway00.audit.AuditSender;
import company.vk.edu.distrib.compute.nihuaway00.audit.SyncAuditSender;
import company.vk.edu.distrib.compute.nihuaway00.transport.http.EntityHttpHandler;
import company.vk.edu.distrib.compute.nihuaway00.transport.grpc.InternalGrpcService;
import company.vk.edu.distrib.compute.nihuaway00.transport.http.StatusHttpHandler;
import company.vk.edu.distrib.compute.nihuaway00.transport.kafka.KafkaEventProducer;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.protobuf.services.ProtoReflectionService;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class NodeServer implements company.vk.edu.distrib.compute.ReplicatedService, AuditableKVService {
    private final KVCommandService commandService;

    private final Properties kafkaProducerProps = new Properties();
    private KafkaEventProducer kafkaEventProducer;
    private AuditSender auditSender;

    private HttpServer httpServer;
    private Server grpcServer;
    private final int port;
    private final int grpcPort;

    public NodeServer(int port, KVCommandService commandService) {
        this(port, port + 1, commandService);
    }

    public NodeServer(int port, int grpcPort, KVCommandService commandService) {
        this.port = port;
        this.grpcPort = grpcPort;
        this.commandService = commandService;
    }

    @Override
    public synchronized void start() {
        if (httpServer != null || grpcServer != null) {
            return;
        }

        try {
            grpcServer = Grpc.newServerBuilderForPort(grpcPort, InsecureServerCredentials.create())
                    .addService(new InternalGrpcService(commandService))
                    .addService(ProtoReflectionService.newInstance())
                    .build();
            grpcServer.start();

            kafkaProducerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            kafkaProducerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            kafkaEventProducer = new KafkaEventProducer(kafkaProducerProps);

            if(auditSender == null){
                auditSender = new SyncAuditSender(kafkaEventProducer);
            }

            httpServer = HttpServer.create(new InetSocketAddress(port), 0);
            httpServer.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
            registerContexts();
            httpServer.start();
        } catch (Exception e) {
            stop();
            throw new IllegalStateException("Failed to start node on port " + port, e);
        }
    }

    @Override
    public synchronized void stop() {
        if (grpcServer != null) {
            Server serverToStop = grpcServer;
            grpcServer = null;
            serverToStop.shutdown();
            try {
                if (!serverToStop.awaitTermination(10, TimeUnit.SECONDS)) {
                    serverToStop.shutdownNow();
                    serverToStop.awaitTermination(10, TimeUnit.SECONDS);
                }
            } catch (InterruptedException e) {
                serverToStop.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        if (kafkaEventProducer != null) {
            kafkaEventProducer.close();
            kafkaEventProducer = null;
        }

        if (httpServer != null) {
            httpServer.stop(0);
            httpServer = null;
        }
    }

    private void registerContexts() {
        httpServer.createContext("/v0/entity", new EntityHttpHandler(commandService, auditSender));
        httpServer.createContext("/v0/status", new StatusHttpHandler());
    }

    @Override
    public int port() {
        return port;
    }

    @Override
    public int numberOfReplicas() {
        return commandService.replicaManager.numberOfReplicas();
    }

    @Override
    public void disableReplica(int nodeId) {
        commandService.replicaManager.disableReplica(nodeId);
    }

    @Override
    public void enableReplica(int nodeId) {
        commandService.replicaManager.enableReplica(nodeId);
    }

    @Override
    public void setBootstrapServers(String bootstrapServers) {
        kafkaProducerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    }

    @Override
    public void setAsync(boolean enabled) {
        if (enabled) {
            kafkaProducerProps.put(ProducerConfig.ACKS_CONFIG, "0");
            this.auditSender = new AsyncAuditSender(kafkaEventProducer);
        } else {
            kafkaProducerProps.put(ProducerConfig.ACKS_CONFIG, "1");
            this.auditSender = new SyncAuditSender(kafkaEventProducer);
        }
    }
}
