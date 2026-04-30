package company.vk.edu.distrib.compute.nihuaway00.transport.grpc;

import com.google.protobuf.ByteString;
import company.vk.edu.distrib.compute.nihuaway00.app.KVCommandService;
import company.vk.edu.distrib.compute.nihuaway00.proto.Key;
import company.vk.edu.distrib.compute.nihuaway00.proto.KeyValue;
import company.vk.edu.distrib.compute.nihuaway00.proto.ReactorKVServiceGrpc;
import company.vk.edu.distrib.compute.nihuaway00.proto.Response;
import io.grpc.Status;
import reactor.core.publisher.Mono;

import java.util.NoSuchElementException;

public class InternalGrpcService extends ReactorKVServiceGrpc.KVServiceImplBase {
    private final KVCommandService commandService;

    public InternalGrpcService(KVCommandService commandService) {
        super();
        this.commandService = commandService;
    }

    @Override
    public Mono<Response> get(Key request) {
        try {
            byte[] data = commandService.handleGetEntity(request.getKey(),
                    request.getAck());
            return Mono.just(Response.newBuilder()
                    .setStatus(200)
                    .setValue(ByteString.copyFrom(data))
                    .build());
        } catch (NoSuchElementException e) {
            return Mono.error(Status.NOT_FOUND.asRuntimeException());
        } catch (IllegalArgumentException e) {
            return Mono.error(Status.INVALID_ARGUMENT.asRuntimeException());
        } catch (Exception e) {
            return Mono.error(Status.INTERNAL.asRuntimeException());
        }
    }

    @Override
    public Mono<Response> upsert(KeyValue request) {
        try {
            commandService.handlePutEntity(request.getKey(), request.getValue().toByteArray(), request.getAck());
            return Mono.just(Response.newBuilder().setStatus(201).build());
        } catch (NoSuchElementException e) {
            return Mono.error(Status.NOT_FOUND.asRuntimeException());
        } catch (IllegalArgumentException e) {
            return Mono.error(Status.INVALID_ARGUMENT.asRuntimeException());
        } catch (Exception e) {
            return Mono.error(Status.INTERNAL.asRuntimeException());
        }
    }

    @Override
    public Mono<Response> delete(Key request) {
        try {
            commandService.handleDeleteEntity(request.getKey(), request.getAck());
            return Mono.just(Response.newBuilder().setStatus(202).build());
        } catch (NoSuchElementException e) {
            return Mono.error(Status.NOT_FOUND.asRuntimeException());
        } catch (IllegalArgumentException e) {
            return Mono.error(Status.INVALID_ARGUMENT.asRuntimeException());
        } catch (Exception e) {
            return Mono.error(Status.INTERNAL.asRuntimeException());
        }
    }

}
