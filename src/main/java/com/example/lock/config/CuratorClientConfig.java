package com.example.lock.config;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CuratorClientConfig {

    @Bean(destroyMethod = "close")
    public CuratorFramework curatorFramework(ZkProperties properties) {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(
                properties.getBaseSleepTimeMs(),
                properties.getMaxRetries());

        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(properties.getConnectString())
                .sessionTimeoutMs(properties.getSessionTimeoutMs())
                .connectionTimeoutMs(properties.getConnectionTimeoutMs())
                .retryPolicy(retryPolicy)
                .build();

        curatorFramework.start();
        return curatorFramework;
    }
}
