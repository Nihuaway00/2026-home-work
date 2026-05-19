package company.vk.edu.distrib.compute.nihuaway00.audit;

import company.vk.edu.distrib.compute.AuditEvent;

public interface AuditSender {
    void send(AuditEvent event);
}
