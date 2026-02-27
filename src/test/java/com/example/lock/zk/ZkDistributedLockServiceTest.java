package com.example.lock.zk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

@ExtendWith(MockitoExtension.class)
class ZkDistributedLockServiceTest {

    @Mock
    private CuratorFramework curatorFramework;
    @Mock
    private InterProcessMutexFactory mutexFactory;
    @Mock
    private ZkConnectionStateMonitor connectionStateMonitor;
    @Mock
    private InterProcessMutex lock;

    private SimpleMeterRegistry meterRegistry;
    private ZkDistributedLockService service;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        service = new ZkDistributedLockService(
                curatorFramework,
                mutexFactory,
                connectionStateMonitor,
                meterRegistry,
                "test-app");
    }

    @Test
    void lockAcquired_taskExecuted_releaseCalled_successOutcome() throws Exception {
        when(connectionStateMonitor.isUnsafeToProceed()).thenReturn(false);
        when(mutexFactory.create(curatorFramework, "/locks/test-app/jobA")).thenReturn(lock);
        when(lock.acquire(anyLong(), eq(TimeUnit.MILLISECONDS))).thenReturn(true);
        when(lock.isAcquiredInThisProcess()).thenReturn(true);

        AtomicBoolean executed = new AtomicBoolean(false);
        boolean result = service.tryWithLock("jobA", Duration.ofMillis(500), Duration.ofSeconds(2), () -> {
            executed.set(true);
            return null;
        });

        assertTrue(result);
        assertTrue(executed.get());
        verify(lock).release();
        assertEquals(1.0, meterRegistry.get("scheduled_job_runs_total")
                .tags("job", "jobA", "outcome", "SUCCESS")
                .counter().count());
    }

    @Test
    void lockNotAcquired_taskSkipped_skippedOutcome() throws Exception {
        when(connectionStateMonitor.isUnsafeToProceed()).thenReturn(false);
        when(mutexFactory.create(curatorFramework, "/locks/test-app/jobB")).thenReturn(lock);
        when(lock.acquire(anyLong(), eq(TimeUnit.MILLISECONDS))).thenReturn(false);

        AtomicBoolean executed = new AtomicBoolean(false);
        boolean result = service.tryWithLock("jobB", Duration.ofMillis(100), Duration.ofSeconds(1), () -> {
            executed.set(true);
            return null;
        });

        assertFalse(result);
        assertFalse(executed.get());
        verify(lock, never()).release();
        assertEquals(1.0, meterRegistry.get("scheduled_job_runs_total")
                .tags("job", "jobB", "outcome", "SKIPPED")
                .counter().count());
        assertEquals(1.0, meterRegistry.get("scheduled_job_skipped_total")
                .tags("job", "jobB")
                .counter().count());
    }

    @Test
    void taskThrowsException_releaseCalled_failOutcome() throws Exception {
        when(connectionStateMonitor.isUnsafeToProceed()).thenReturn(false);
        when(mutexFactory.create(curatorFramework, "/locks/test-app/jobC")).thenReturn(lock);
        when(lock.acquire(anyLong(), eq(TimeUnit.MILLISECONDS))).thenReturn(true);
        when(lock.isAcquiredInThisProcess()).thenReturn(true);

        boolean result = service.tryWithLock("jobC", Duration.ofMillis(100), Duration.ofSeconds(1), (Callable<Void>) () -> {
            throw new IllegalStateException("boom");
        });

        assertFalse(result);
        verify(lock).release();
        assertEquals(1.0, meterRegistry.get("scheduled_job_runs_total")
                .tags("job", "jobC", "outcome", "FAIL")
                .counter().count());
    }

    @Test
    void unsafeBeforeStart_taskNotExecuted_unsafeOutcome() {
        when(connectionStateMonitor.isUnsafeToProceed()).thenReturn(true);

        AtomicBoolean executed = new AtomicBoolean(false);
        boolean result = service.tryWithLock("jobD", Duration.ofMillis(100), Duration.ofSeconds(1), () -> {
            executed.set(true);
            return null;
        });

        assertFalse(result);
        assertFalse(executed.get());
        verify(mutexFactory, never()).create(curatorFramework, "/locks/test-app/jobD");
        assertEquals(1.0, meterRegistry.get("scheduled_job_runs_total")
                .tags("job", "jobD", "outcome", "UNSAFE")
                .counter().count());
    }

    @Test
    void timeoutByLockAtMostFor_taskCancelled_failOutcome() throws Exception {
        when(connectionStateMonitor.isUnsafeToProceed()).thenReturn(false);
        when(mutexFactory.create(curatorFramework, "/locks/test-app/jobE")).thenReturn(lock);
        when(lock.acquire(anyLong(), eq(TimeUnit.MILLISECONDS))).thenReturn(true);
        when(lock.isAcquiredInThisProcess()).thenReturn(true);

        AtomicBoolean interrupted = new AtomicBoolean(false);
        boolean result = service.tryWithLock("jobE", Duration.ofMillis(100), Duration.ofMillis(150), () -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                interrupted.set(true);
                Thread.currentThread().interrupt();
            }
            return null;
        });

        assertFalse(result);
        verify(lock).release();
        assertTrue(interrupted.get());
        assertEquals(1.0, meterRegistry.get("scheduled_job_runs_total")
                .tags("job", "jobE", "outcome", "FAIL")
                .counter().count());
    }
}
