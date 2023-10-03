package com.apple.aml.stargate.agent.config;

import com.apple.aml.stargate.agent.data.CaffeineMetadataCache;
import com.apple.aml.stargate.agent.data.LocalCache;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

@Configuration
public class BeanConfiguration {

    @Value("${stargate.observability.agent.application.threadpool.corePoolSize}")
    private int corePoolSize;

    @Value("${stargate.observability.agent.application.threadpool.maxPoolSize}")
    private int maxPoolSize;

    @Value("${stargate.observability.agent.application.threadpool.queueCapacity}")
    private int queueCapacity;

    @Bean(name = "threadPoolExecutor")
    public Executor getAsyncExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(corePoolSize);
        executor.setMaxPoolSize(maxPoolSize);
        executor.setQueueCapacity(queueCapacity);
        executor.setThreadNamePrefix("threadPoolExecutor-");
        executor.initialize();
        return executor;
    }
}
