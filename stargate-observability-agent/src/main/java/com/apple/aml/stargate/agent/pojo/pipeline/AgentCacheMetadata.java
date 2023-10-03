package com.apple.aml.stargate.agent.pojo.pipeline;

import lombok.Data;

import java.io.Serializable;

@Data
public class AgentCacheMetadata implements Serializable {
    private static final long serialVersionUID = 1L;

    private PipelineMetadata pipelineMetadata;

    private boolean isSyncedWithAdminApp;
}
