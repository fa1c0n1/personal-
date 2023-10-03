package com.apple.aml.stargate.agent.constants;

public interface Constants {
    interface Prometheus {
        String QUERY = "query";
        String QUERY_RANGE_CONTROLLER = "query_range";
        String METADATA_CONTROLLER = "targets/metadata";
        String GET_ALL_METRICS_CONTROLLER = "label/__name__/values";

        String QUERY_PARAM_START_DATE = "start";

        String QUERY_PARAM_END_DATE = "end";
        String QUERY_PARAM_STEP = "step";
    }

    interface AdminAppController {
        String UPDATE_PIPELINE_METADATA = "api/v1/pipeline/deployment/set/app/metadata/%s";
        String GET_PIPELINE_SPEC = "sg/pipeline/spec/%s";
        String GET_PIPELINE_METADATA = "api/v1/pipeline/deployment/get/app/pipelineInfo/%s";
    }
}
