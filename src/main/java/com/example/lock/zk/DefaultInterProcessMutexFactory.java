package com.example.lock.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.springframework.stereotype.Component;

@Component
public class DefaultInterProcessMutexFactory implements InterProcessMutexFactory {

    @Override
    public InterProcessMutex create(CuratorFramework client, String lockPath) {
        return new InterProcessMutex(client, lockPath);
    }
}
