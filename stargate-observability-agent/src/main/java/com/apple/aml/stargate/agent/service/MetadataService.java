package com.apple.aml.stargate.agent.service;

import com.apple.aml.stargate.agent.pojo.pipeline.AgentCacheMetadata;

public interface MetadataService {
    boolean updatePipelineMetadata(String pipelineId, AgentCacheMetadata cacheMetadata);

    String getPipelineMetadata(String pipelineId);
}
