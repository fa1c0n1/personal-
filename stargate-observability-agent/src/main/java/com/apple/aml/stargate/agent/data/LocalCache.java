package com.apple.aml.stargate.agent.data;

import com.apple.aml.stargate.agent.pojo.pipeline.AgentCacheMetadata;

public interface LocalCache {

    AgentCacheMetadata getPipelineMetadata(String pipelineId);

    boolean setPipelineMetadata(String pipelineId, AgentCacheMetadata cacheMetadata);
}
