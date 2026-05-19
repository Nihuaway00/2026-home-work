package company.vk.edu.distrib.compute.nihuaway00.audit;

import company.vk.edu.distrib.compute.AuditEvent;

@FunctionalInterface
public interface AuditSender {
    void send(AuditEvent event);
}
