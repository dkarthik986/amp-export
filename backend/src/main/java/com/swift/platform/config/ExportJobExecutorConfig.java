package com.swift.platform.config;

import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

@Configuration
@RequiredArgsConstructor
public class ExportJobExecutorConfig {

    private final AppConfig appConfig;

    @Bean(name = "exportJobExecutor")
    public Executor exportJobExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setThreadNamePrefix("export-job-");
        executor.setCorePoolSize(Math.max(1, appConfig.getExportExecutorCorePoolSize()));
        executor.setMaxPoolSize(Math.max(executor.getCorePoolSize(), appConfig.getExportExecutorMaxPoolSize()));
        executor.setQueueCapacity(Math.max(1, appConfig.getExportExecutorQueueCapacity()));
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.initialize();
        return executor;
    }
}
