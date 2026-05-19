package company.vk.edu.distrib.compute.nihuaway00.audit;

import company.vk.edu.distrib.compute.AuditEvent;

public class NoOpAuditSender implements AuditSender {
    @Override
    public void send(AuditEvent event) {
        // отдыхает и ничего не делает. Вариация когда аудит не нужен
    }
}
