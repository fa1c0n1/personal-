package com.apple.aml.stargate.app.routes;

import com.apple.aml.stargate.app.pojo.ModelCleanupPipeline;
import com.apple.aml.stargate.app.pojo.ModelPipeline;
import com.apple.aml.stargate.app.service.IngestionService;
import com.apple.aml.stargate.common.pojo.ResponseBody;
import com.apple.appeng.aluminum.auth.spring.security.reactive.AuthenticatedPrincipalProvider;
import io.micrometer.core.annotation.Timed;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import static com.apple.aml.stargate.common.constants.CommonConstants.API_PREFIX;

@Timed
@RestController
@RequestMapping(API_PREFIX + "/ingestion")
//@Api(value = "Ingestion Service Router")
public class IngestionRouter {
    @Autowired
    private IngestionService ingestionService;
    @Autowired
    private AuthenticatedPrincipalProvider authProvider;

    @PostMapping(value = "/create-model-pipeline")
    //@ApiOperation(value = "Description here", response = ResponseBody.class)
    public Mono<ResponseBody> createModelPipeline(@RequestBody final ModelPipeline pipeline) {
        return authProvider.retrieveApplication().map(app -> app.getAppId()).flatMap(appId -> ingestionService.createModelPipeline(appId, pipeline));
    }

    @PostMapping(value = "/remove-model-pipeline")
    //@ApiOperation(value = "Description here", response = ResponseBody.class)
    public Mono<ResponseBody> removeModelPipeline(@RequestBody final ModelCleanupPipeline pipeline) {
        return authProvider.retrieveApplication().map(app -> app.getAppId()).flatMap(appId -> ingestionService.removeModelPipeline(appId, pipeline));
    }

    @DeleteMapping(value = "/internal/remove-kafka-topic/group/{groupId}/namespace/{namespaceId}/topic/{topic}")
    //@ApiOperation(value = "Description here", response = ResponseBody.class)
    //Assuming fullTopicName is group.namespace.topic
    public Mono<ResponseBody> removeKafkaTopic(@PathVariable final String groupId, @PathVariable final String namespaceId, @PathVariable final String topic) {
        return authProvider.retrieveApplication().map(app -> app.getAppId()).flatMap(appId -> ingestionService.removeKafkaTopic(appId, groupId, namespaceId, topic));
    }
}
