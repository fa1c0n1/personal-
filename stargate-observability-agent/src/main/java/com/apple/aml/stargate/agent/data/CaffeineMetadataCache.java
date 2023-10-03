package com.apple.aml.stargate.agent.data;

import com.apple.aml.stargate.agent.pojo.pipeline.AgentCacheMetadata;
import com.apple.aml.stargate.agent.pojo.pipeline.PipelineMetadata;
import com.apple.aml.stargate.agent.pojo.pipeline.AgentStargatePipeline;
import com.apple.aml.stargate.agent.service.MetadataService;
import com.apple.aml.stargate.common.utils.JsonUtils;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;

import static com.apple.aml.stargate.common.utils.LogUtils.logger;

@Service
public class CaffeineMetadataCache implements LocalCache {

    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    @Autowired
    private MetadataService metadataService;

    @Value("${stargate.observability.agent.cache.size}")
    private int cacheSize;

    @Value("${stargate.observability.agent.cache.expireAfterAccess}")
    private long expireAfterAccess;

    private Cache<String, AgentCacheMetadata> cache;

    @PostConstruct
    public void initializeCache() {
        cache = Caffeine.newBuilder()
                .expireAfterAccess(expireAfterAccess, TimeUnit.SECONDS)
                .maximumSize(cacheSize)
                .build();
    }

    @Override
    public AgentCacheMetadata getPipelineMetadata(String pipelineId) {
        return cache.get(pipelineId, k -> getCachePipelineMetadata(pipelineId));
    }

    @Override
    public boolean setPipelineMetadata(String pipelineId, AgentCacheMetadata cacheMetadata) {
        return false;
    }

    private AgentCacheMetadata getCachePipelineMetadata(String pipelineId) {
        String pipeline = metadataService.getPipelineMetadata(pipelineId);
        return parsePipelineMetadata(pipeline);

    }

    private AgentCacheMetadata parsePipelineMetadata(String pipeline) {

        if(pipeline == null) {
            return createEmptyCacheMetadata();
        }
        AgentStargatePipeline stargatePipeline = null;
        try {
            stargatePipeline = JsonUtils.readJson(pipeline, AgentStargatePipeline.class);
        } catch (Exception e) {
            LOGGER.error("Exception in parsing Pipeline JSON", e);
        }
        if (stargatePipeline != null && stargatePipeline.getPipelineMetadata() != null) {
            AgentCacheMetadata cacheMetadata = new AgentCacheMetadata();
            cacheMetadata.setPipelineMetadata(stargatePipeline.getPipelineMetadata());
            return cacheMetadata;
        } else {
           return createEmptyCacheMetadata();
        }
    }

    private AgentCacheMetadata createEmptyCacheMetadata(){
        AgentCacheMetadata cacheMetadata = new AgentCacheMetadata();
        cacheMetadata.setPipelineMetadata(new PipelineMetadata());
        return cacheMetadata;
    }
}
