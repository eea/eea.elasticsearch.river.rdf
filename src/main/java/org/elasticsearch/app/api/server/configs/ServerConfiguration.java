package org.elasticsearch.app.api.server.configs;

import org.elasticsearch.app.Indexer;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@Configuration
@EnableCaching
public class ServerConfiguration {

    @Bean
    public ThreadPoolTaskScheduler threadPoolTaskScheduler() {
        ThreadPoolTaskScheduler threadPoolTaskScheduler = new ThreadPoolTaskScheduler();
        threadPoolTaskScheduler.setPoolSize(1);
        threadPoolTaskScheduler.setRemoveOnCancelPolicy(true);
        threadPoolTaskScheduler.setThreadNamePrefix("TaskScheduler:");
        return threadPoolTaskScheduler;
    }

    @Bean
    @Scope("singleton")
    public Indexer indexer() {
        return new Indexer();
    }


    @Bean
    public CacheManager cacheManager() {
        return new ConcurrentMapCacheManager("dashboardsInfo");
    }
}
