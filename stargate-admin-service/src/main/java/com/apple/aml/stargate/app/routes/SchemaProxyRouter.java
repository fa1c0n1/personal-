package com.apple.aml.stargate.app.routes;

import com.apple.aml.stargate.app.service.SchemaProxyService;
import com.apple.aml.stargate.common.pojo.ResponseBody;
import com.apple.appeng.aluminum.auth.spring.security.reactive.AuthenticatedPrincipalProvider;
import io.micrometer.core.annotation.Timed;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.lang.invoke.MethodHandles;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.API_PREFIX;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static java.lang.Long.parseLong;

@Timed
@RestController
@RequestMapping(API_PREFIX + "/schemas")
//@Api(value = "Schema Proxy Router")
public class SchemaProxyRouter {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    @Autowired
    private AuthenticatedPrincipalProvider authProvider;
    @Autowired
    private SchemaProxyService schemaProxyService;

    @PostMapping(value = "/register")
    //@ApiOperation(value = "Registers/Updates the supplied schemas with AppleSchemaStore", response = ResponseBody.class)
    public Mono<ResponseBody> registerSchemas(@RequestHeader(value = "X-Schema-Author", required = false) final String schemaAuthor, @RequestHeader(value = "X-Schema-AllowBreakingFullAPICompatibility", required = false, defaultValue = "false") final boolean allowBreakingFullAPICompatibility, @RequestHeader(value = "X-Schema-AllowBreakingFieldOrder", required = false, defaultValue = "false") final boolean allowBreakingFieldOrder, @RequestBody final Object schemas, final ServerHttpRequest request) {
        return authProvider.retrieveApplication().map(app -> parseLong(app.getAppId())).flatMap(appId -> schemaProxyService.registerSchemas(appId, schemas, schemaAuthor, allowBreakingFullAPICompatibility, allowBreakingFieldOrder, request));
    }

    @PostMapping(value = "/register/csv/{schemaId}")
    //@ApiOperation(value = "Registers/Updates the supplied schemas with AppleSchemaStore", response = ResponseBody.class)
    public Mono<ResponseBody> registerCSVSchema(@PathVariable final String schemaId, @RequestHeader(value = "X-Schema-Author", required = false) final String schemaAuthor, @RequestHeader(value = "X-Schema-AllowBreakingFullAPICompatibility", required = false, defaultValue = "false") final boolean allowBreakingFullAPICompatibility, @RequestHeader(value = "X-Schema-AllowBreakingFieldOrder", required = false, defaultValue = "false") final boolean allowBreakingFieldOrder, @RequestHeader(value = "X-Schema-IncludesHeader", required = false) final boolean includesHeader, @RequestBody final String csv, final ServerHttpRequest request) {
        return authProvider.retrieveApplication().map(app -> parseLong(app.getAppId())).flatMap(appId -> schemaProxyService.registerCSVSchema(appId, schemaId, csv, schemaAuthor, allowBreakingFullAPICompatibility, allowBreakingFieldOrder, includesHeader, request));
    }

    @PostMapping(value = "/register/eai")
    //@ApiOperation(value = "Registers/Updates the supplied schemas with AppleSchemaStore & creates additional EAI Augmented Schemas", response = ResponseBody.class)
    public Mono<ResponseBody> registerEAISchemas(@RequestHeader(value = "X-Schema-Author", required = false) final String schemaAuthor, @RequestHeader(value = "X-Schema-AllowBreakingFullAPICompatibility", required = false, defaultValue = "false") final boolean allowBreakingFullAPICompatibility, @RequestHeader(value = "X-Schema-AllowBreakingFieldOrder", required = false, defaultValue = "false") final boolean allowBreakingFieldOrder, @RequestBody final Object schemas, final ServerHttpRequest request) {
        return authProvider.retrieveApplication().map(app -> parseLong(app.getAppId())).flatMap(appId -> schemaProxyService.registerEAISchemas(appId, schemas, schemaAuthor, allowBreakingFullAPICompatibility, allowBreakingFieldOrder, request));
    }

    @GetMapping(value = "/fetch/{schemaId}")
    //@ApiOperation(value = "Fetch the latest schema associated with supplied schemaId from AppleSchemaStore", response = String.class)
    public Mono<Map<String, Object>> fetchSchema(@PathVariable final String schemaId, final ServerHttpRequest request) {
        return authProvider.retrieveApplication().map(app -> parseLong(app.getAppId())).flatMap(appId -> schemaProxyService.fetchLatestSchema(appId, schemaId, request));
    }

    @GetMapping(value = "/fetch/{schemaId}/{versionNo}")
    //@ApiOperation(value = "Fetch the schema associated with supplied schemaId & version from AppleSchemaStore", response = String.class)
    public Mono<Map<String, Object>> fetchSchema(@PathVariable final String schemaId, @PathVariable final int versionNo, final ServerHttpRequest request) {
        return authProvider.retrieveApplication().map(app -> parseLong(app.getAppId())).flatMap(appId -> schemaProxyService.fetchSchema(appId, schemaId, versionNo, request));
    }
}
