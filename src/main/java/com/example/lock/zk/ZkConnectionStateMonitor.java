package com.example.lock.zk;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class ZkConnectionStateMonitor {

    private static final Logger log = LoggerFactory.getLogger(ZkConnectionStateMonitor.class);

    private final AtomicBoolean unsafeToProceed = new AtomicBoolean(false);

    public ZkConnectionStateMonitor(CuratorFramework curatorFramework) {
        ConnectionStateListener listener = (client, newState) -> onStateChanged(newState);
        curatorFramework.getConnectionStateListenable().addListener(listener);
    }

    private void onStateChanged(ConnectionState state) {
        if (state == ConnectionState.SUSPENDED || state == ConnectionState.LOST) {
            unsafeToProceed.set(true);
            log.warn("ZooKeeper connection state changed to {}. Marking scheduler as unsafe.", state);
            return;
        }

        if (state == ConnectionState.RECONNECTED || state == ConnectionState.CONNECTED) {
            unsafeToProceed.set(false);
            log.info("ZooKeeper connection state changed to {}. Scheduler is safe again.", state);
        }
    }

    public boolean isUnsafeToProceed() {
        return unsafeToProceed.get();
    }
}
