package company.vk.edu.distrib.compute.nihuaway00.bootstrap;

import company.vk.edu.distrib.compute.AuditService;

import java.io.IOException;

public class AuditServiceFactory extends company.vk.edu.distrib.compute.AuditServiceFactory {
    @Override
    protected AuditService doCreate(String bootstrapServers, String consumerGroupId) throws IOException {
        return new company.vk.edu.distrib.compute.nihuaway00.audit.AuditService(bootstrapServers, consumerGroupId);
    }
}
