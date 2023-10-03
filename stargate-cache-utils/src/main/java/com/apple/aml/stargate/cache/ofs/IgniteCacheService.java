package com.apple.aml.stargate.cache.ofs;

import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

@Service
@ConditionalOnExpression("'${stargate.cache.ofs.type}'.startsWith('ignite-backed-by-')")
public class IgniteCacheService extends IgniteLocalCacheService implements CacheService {
    @PostConstruct
    public void init() {
        super.init();
    }
}