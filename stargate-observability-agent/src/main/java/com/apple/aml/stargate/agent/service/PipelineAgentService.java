package com.apple.aml.stargate.agent.service;

import com.apple.aml.stargate.agent.constants.Constants;
import com.apple.aml.stargate.agent.pojo.pipeline.AgentCacheMetadata;
import com.apple.aml.stargate.common.constants.A3Constants;
import com.apple.aml.stargate.common.utils.A3Utils;
import com.apple.aml.stargate.common.utils.AppConfig;
import com.apple.aml.stargate.common.utils.WebUtils;
import okhttp3.MediaType;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;

import java.lang.invoke.MethodHandles;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.HEADER_A3_TOKEN;
import static com.apple.aml.stargate.common.constants.CommonConstants.HEADER_CLIENT_APP_ID;
import static com.apple.aml.stargate.common.utils.AppConfig.environment;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;

@Service
public class PipelineAgentService implements MetadataService {

    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    public boolean updatePipelineMetadata(String pipelineId, AgentCacheMetadata cacheMetadata) {

        String url = environment().getConfig().getStargateUri() + "/" + Constants.AdminAppController.UPDATE_PIPELINE_METADATA;
        String finalUrl = String.format(url, pipelineId);
        try {
            String response = WebUtils.httpPost(finalUrl
                    , cacheMetadata.getPipelineMetadata()
                    , MediaType.get("application/json")
                    , Map.of(HEADER_CLIENT_APP_ID
                            , String.valueOf(AppConfig.appId())
                            , HEADER_A3_TOKEN
                            , A3Utils.getA3Token(A3Constants.KNOWN_APP.STARGATE.appId()))
                    , String.class, true, false);
            LOGGER.debug(response);
            Map<Object, Object> responseMap = readJsonMap(response);
            cacheMetadata.setSyncedWithAdminApp(responseMap.containsKey("dataCatalogUpdate")? (boolean) responseMap.get("dataCatalogUpdate") : true);
        } catch (Exception e) {
            cacheMetadata.setSyncedWithAdminApp(false);
            LOGGER.error(String.format("Error in updating Pipeline Metadata with Stargate Admin App for pipelineId %s", pipelineId), e);
        }

        return false;
    }

    public String getPipelineMetadata(String pipelineId) {

        String url = environment().getConfig().getStargateUri() + "/" + Constants.AdminAppController.GET_PIPELINE_METADATA;
        String finalUrl = String.format(url, pipelineId);
        try {
            String response = WebUtils.httpGet(finalUrl
                    , Map.of(HEADER_CLIENT_APP_ID
                            , String.valueOf(AppConfig.appId())
                            , HEADER_A3_TOKEN
                            , A3Utils.getA3Token(A3Constants.KNOWN_APP.STARGATE.appId()))
                    , String.class, true, true);
            LOGGER.debug("Response for PipelineMetadata - " + response);
            return response;
        } catch (Exception e) {
            LOGGER.error(String.format("Error in getting Pipeline Metadata with Stargate Admin App for pipelineId %s", pipelineId), e);
        }

        return null;
    }
}
