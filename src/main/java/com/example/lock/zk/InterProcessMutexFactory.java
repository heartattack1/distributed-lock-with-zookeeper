package com.example.lock.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

public interface InterProcessMutexFactory {

    InterProcessMutex create(CuratorFramework client, String lockPath);
}
