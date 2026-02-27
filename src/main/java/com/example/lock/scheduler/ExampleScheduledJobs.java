package com.example.lock.scheduler;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.example.lock.zk.ZkDistributedLockService;

@Component
public class ExampleScheduledJobs {

    private static final Logger log = LoggerFactory.getLogger(ExampleScheduledJobs.class);

    private final ZkDistributedLockService distributedLockService;

    public ExampleScheduledJobs(ZkDistributedLockService distributedLockService) {
        this.distributedLockService = distributedLockService;
    }

    @Scheduled(fixedDelayString = "${app.jobs.cleanup.fixed-delay-ms:30000}")
    public void cleanupJob() {
        String jobName = "cleanupJob";
        distributedLockService.tryWithLock(
                jobName,
                Duration.ofSeconds(2),
                Duration.ofSeconds(20),
                () -> {
                    log.info("Executing cleanup job logic");
                    simulateWork(1000);
                });
    }

    @Scheduled(cron = "${app.jobs.reports.cron:0 */1 * * * *}")
    public void reportsJob() {
        String jobName = "reportsJob";
        distributedLockService.tryWithLock(
                jobName,
                Duration.ofSeconds(2),
                Duration.ofSeconds(30),
                () -> {
                    log.info("Executing reports job logic");
                    simulateWork(1500);
                });
    }

    private void simulateWork(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Job interrupted");
        }
    }
}
