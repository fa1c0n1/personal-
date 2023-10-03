package com.apple.aml.stargate.cache.ofs;

import com.apple.aml.stargate.cache.utils.RedisUtils;
import org.redisson.api.RedissonClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CacheExtConfig {
    @Bean
    @ConditionalOnExpression("'${stargate.cache.ofs.type}'.startsWith('redis')")
    public RedissonClient redisClient() throws Exception {
        return RedisUtils.initRedisClient();
    }

}
