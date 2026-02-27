package com.example.lock.zk;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.PreDestroy;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

@Service
public class ZkDistributedLockService {

    private static final Logger log = LoggerFactory.getLogger(ZkDistributedLockService.class);
    private static final long FUTURE_POLL_INTERVAL_MS = 200L;

    private final CuratorFramework curatorFramework;
    private final InterProcessMutexFactory mutexFactory;
    private final ZkConnectionStateMonitor connectionStateMonitor;
    private final MeterRegistry meterRegistry;
    private final ExecutorService executorService;
    private final String applicationName;
    private final Map<String, Future<?>> runningTasks = new ConcurrentHashMap<>();

    public ZkDistributedLockService(CuratorFramework curatorFramework,
                                    InterProcessMutexFactory mutexFactory,
                                    ZkConnectionStateMonitor connectionStateMonitor,
                                    MeterRegistry meterRegistry,
                                    @Value("${spring.application.name:application}") String applicationName) {
        this.curatorFramework = curatorFramework;
        this.mutexFactory = mutexFactory;
        this.connectionStateMonitor = connectionStateMonitor;
        this.meterRegistry = meterRegistry;
        this.applicationName = applicationName;
        this.executorService = Executors.newCachedThreadPool(new LockTaskThreadFactory());
    }

    public boolean tryWithLock(String jobName,
                               Duration acquireTimeout,
                               Duration lockAtMostFor,
                               Runnable task) {
        Objects.requireNonNull(task, "task must not be null");
        return tryWithLock(jobName, acquireTimeout, lockAtMostFor, () -> {
            task.run();
            return null;
        });
    }

    public boolean tryWithLock(String jobName,
                               Duration acquireTimeout,
                               Duration lockAtMostFor,
                               Callable<Void> task) {
        Objects.requireNonNull(jobName, "jobName must not be null");
        Objects.requireNonNull(acquireTimeout, "acquireTimeout must not be null");
        Objects.requireNonNull(lockAtMostFor, "lockAtMostFor must not be null");
        Objects.requireNonNull(task, "task must not be null");

        String lockPath = "/locks/" + applicationName + "/" + jobName;
        if (connectionStateMonitor.isUnsafeToProceed()) {
            recordOutcome(jobName, "UNSAFE");
            recordSkipped(jobName);
            log.warn("Skip job due to unsafe ZooKeeper state: jobName={}, lockPath={}, outcome=UNSAFE",
                    jobName, lockPath);
            return false;
        }

        InterProcessMutex lock = mutexFactory.create(curatorFramework, lockPath);
        boolean acquired = false;
        long acquireStartNs = System.nanoTime();
        long acquireTimeMs = 0L;

        try {
            acquired = lock.acquire(acquireTimeout.toMillis(), TimeUnit.MILLISECONDS);
            acquireTimeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - acquireStartNs);
            recordAcquireTimer(jobName, acquireStartNs);

            if (!acquired) {
                recordOutcome(jobName, "SKIPPED");
                recordSkipped(jobName);
                log.info("Skip job because lock was not acquired: jobName={}, lockPath={}, acquired={}, acquireTimeMs={}, outcome=SKIPPED",
                        jobName, lockPath, false, acquireTimeMs);
                return false;
            }

            long executionStartNs = System.nanoTime();
            Future<Void> future = executorService.submit(task);
            runningTasks.put(jobName, future);

            try {
                waitForCompletion(jobName, lockPath, lockAtMostFor, executionStartNs, future);
                long executionTimeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - executionStartNs);
                recordExecutionTimer(jobName, executionStartNs);
                recordOutcome(jobName, "SUCCESS");
                log.info("Job completed: jobName={}, lockPath={}, acquired={}, acquireTimeMs={}, executionTimeMs={}, outcome=SUCCESS",
                        jobName, lockPath, true, acquireTimeMs, executionTimeMs);
                return true;
            } catch (TimeoutException e) {
                long executionTimeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - executionStartNs);
                recordExecutionTimer(jobName, executionStartNs);
                recordOutcome(jobName, "FAIL");
                log.error("Job timed out and was cancelled: jobName={}, lockPath={}, acquired={}, acquireTimeMs={}, executionTimeMs={}, outcome=FAIL",
                        jobName, lockPath, true, acquireTimeMs, executionTimeMs, e);
                return false;
            } catch (CancellationException e) {
                long executionTimeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - executionStartNs);
                recordExecutionTimer(jobName, executionStartNs);
                recordOutcome(jobName, "UNSAFE");
                log.warn("Job cancelled due to unsafe state: jobName={}, lockPath={}, acquired={}, acquireTimeMs={}, executionTimeMs={}, outcome=UNSAFE",
                        jobName, lockPath, true, acquireTimeMs, executionTimeMs, e);
                return false;
            } catch (ExecutionException e) {
                long executionTimeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - executionStartNs);
                recordExecutionTimer(jobName, executionStartNs);
                recordOutcome(jobName, "FAIL");
                log.error("Job failed: jobName={}, lockPath={}, acquired={}, acquireTimeMs={}, executionTimeMs={}, outcome=FAIL",
                        jobName, lockPath, true, acquireTimeMs, executionTimeMs, e);
                return false;
            } finally {
                runningTasks.remove(jobName);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            recordOutcome(jobName, "FAIL");
            log.error("Interrupted while acquiring/executing job lock: jobName={}, lockPath={}, acquired={}, acquireTimeMs={}, outcome=FAIL",
                    jobName, lockPath, acquired, acquireTimeMs, e);
            return false;
        } catch (Exception e) {
            recordOutcome(jobName, "FAIL");
            log.error("Error in distributed lock execution: jobName={}, lockPath={}, acquired={}, acquireTimeMs={}, outcome=FAIL",
                    jobName, lockPath, acquired, acquireTimeMs, e);
            return false;
        } finally {
            if (acquired) {
                releaseLock(lock, jobName, lockPath);
            }
        }
    }

    private void waitForCompletion(String jobName,
                                   String lockPath,
                                   Duration lockAtMostFor,
                                   long executionStartNs,
                                   Future<Void> future)
            throws InterruptedException, ExecutionException, TimeoutException {
        long deadlineNs = executionStartNs + lockAtMostFor.toNanos();

        while (true) {
            if (connectionStateMonitor.isUnsafeToProceed()) {
                // InterProcessMutex provides mutual exclusion while the session is healthy, but it is not exactly-once.
                // If the ZooKeeper session is lost, duplicate execution may happen; business actions must be idempotent.
                log.warn("Unsafe ZooKeeper state detected during job execution, cancelling task as best-effort: jobName={}, lockPath={}, outcome=UNSAFE",
                        jobName, lockPath);
                future.cancel(true);
                throw new CancellationException("Unsafe ZooKeeper state");
            }

            long remainingNs = deadlineNs - System.nanoTime();
            if (remainingNs <= 0) {
                // ZK lock has no TTL. lockAtMostFor is a guardrail enforced in-process via Future timeout + interruption.
                future.cancel(true);
                throw new TimeoutException("Task execution exceeded lockAtMostFor");
            }

            long waitMs = Math.min(TimeUnit.NANOSECONDS.toMillis(remainingNs), FUTURE_POLL_INTERVAL_MS);
            if (waitMs <= 0) {
                waitMs = 1;
            }

            try {
                future.get(waitMs, TimeUnit.MILLISECONDS);
                return;
            } catch (TimeoutException ignored) {
                // continue polling until deadline or completion
            }
        }
    }

    private void releaseLock(InterProcessMutex lock, String jobName, String lockPath) {
        try {
            if (lock.isAcquiredInThisProcess()) {
                lock.release();
            }
        } catch (Exception e) {
            log.error("Failed to release lock: jobName={}, lockPath={}", jobName, lockPath, e);
        }
    }

    private void recordAcquireTimer(String jobName, long acquireStartNs) {
        Timer.builder("scheduled_job_lock_acquire_seconds")
                .tag("job", jobName)
                .register(meterRegistry)
                .record(System.nanoTime() - acquireStartNs, TimeUnit.NANOSECONDS);
    }

    private void recordExecutionTimer(String jobName, long executionStartNs) {
        Timer.builder("scheduled_job_execution_seconds")
                .tag("job", jobName)
                .register(meterRegistry)
                .record(System.nanoTime() - executionStartNs, TimeUnit.NANOSECONDS);
    }

    private void recordOutcome(String jobName, String outcome) {
        Counter.builder("scheduled_job_runs_total")
                .tag("job", jobName)
                .tag("outcome", outcome)
                .register(meterRegistry)
                .increment();
    }

    private void recordSkipped(String jobName) {
        Counter.builder("scheduled_job_skipped_total")
                .tag("job", jobName)
                .register(meterRegistry)
                .increment();
    }

    @PreDestroy
    public void shutdown() {
        for (Map.Entry<String, Future<?>> entry : runningTasks.entrySet()) {
            entry.getValue().cancel(true);
            log.warn("Cancelling running job during shutdown: jobName={}", entry.getKey());
        }

        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executorService.shutdownNow();
        }
    }

    private static final class LockTaskThreadFactory implements ThreadFactory {

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setName("zk-lock-task-" + thread.getId());
            thread.setDaemon(true);
            return thread;
        }
    }
}
