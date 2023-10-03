package com.apple.aml.stargate.rm.service;

import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.spring.service.PrometheusService;
import com.apple.aml.stargate.external.rpc.proto.Epoc;
import com.apple.aml.stargate.external.rpc.proto.ExternalFunctionProtoServiceGrpc.ExternalFunctionProtoServiceBlockingStub;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import javax.annotation.PostConstruct;
import java.io.File;
import java.lang.invoke.MethodHandles;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.beam.sdk.ts.ExternalFunction.grpcPools;
import static com.apple.aml.stargate.beam.sdk.ts.ExternalFunction.returnGrpcToPool;
import static com.apple.aml.stargate.common.constants.CommonConstants.EMPTY_STRING;
import static com.apple.aml.stargate.common.constants.CommonConstants.HEADER_RM_HOST;
import static com.apple.aml.stargate.common.constants.CommonConstants.HEADER_RM_INITIALIZATION_VECTOR;
import static com.apple.aml.stargate.common.constants.CommonConstants.HEADER_RM_TOKEN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_DELIMITER;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.pipelineId;
import static com.apple.aml.stargate.common.utils.AppConfig.config;
import static com.apple.aml.stargate.common.utils.EncryptionUtils.defaultDecrypt;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.getSharedDir;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.resourceManagerKeySpec;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Arrays.asList;

@Service
public class RMCoreService {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    public static final AtomicReference<String> PIPELINE_STATUS = new AtomicReference<>("UNKNOWN");
    private static final long tokenValidity = 60 * 1000; // in milliseconds
    private final ConcurrentHashMap<String, Object> stateMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Schema> schemaMap = new ConcurrentHashMap<>();
    @Autowired
    @Lazy
    private PrometheusService prometheusService;

    @PostConstruct
    void init() {
        try {
            for (String fileName : config().getStringList("stargate.avro.schemas.autoload")) {
                String content = IOUtils.resourceToString(String.format("/%s.avsc", fileName), Charset.defaultCharset());
                if (isBlank(content)) {
                    LOGGER.warn("Could not load local schema file. Will skip this file", Map.of("fileName", fileName));
                    continue;
                }
                for (PipelineConstants.ENVIRONMENT environment : PipelineConstants.ENVIRONMENT.values()) {
                    try {
                        String schemaString = content.replaceAll("\\#\\{ENV\\}", environment.name().toLowerCase());
                        Schema.Parser parser = new Schema.Parser();
                        Schema schema = parser.parse(schemaString);
                        schemaMap.put(schema.getFullName(), schema);
                    } catch (Exception e) {
                        LOGGER.warn("Could not auto register local schema", Map.of("environment", environment.name(), "fileName", fileName));
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Mono<ResponseEntity<Resource>> getSharedFile(final String path, final ServerHttpRequest request) {
        return Mono.create(sink -> {
            try {
                authenticate(request);
                String sharedDir = getSharedDir();
                Path filePath = Paths.get(sharedDir, path);
                File file = filePath.toFile();
                checkArgument(file.exists() && file.isFile(), "File not found");
                byte[] bytes = Files.readAllBytes(filePath);
                checkArgument(bytes != null && bytes.length > 0, "Empty/Invalid file found");
                ByteArrayResource resource = new ByteArrayResource(bytes);
                sink.success(ResponseEntity.ok().contentType(MediaType.APPLICATION_OCTET_STREAM).contentLength(resource.contentLength()).header(HttpHeaders.CONTENT_DISPOSITION, ContentDisposition.attachment().filename(path).build().toString()).body(resource));
            } catch (Exception e) {
                LOGGER.warn("Could not send request file", Map.of("path", String.valueOf(path), ERROR_MESSAGE, e.getMessage()), e);
                sink.error(e);
            }
        });
    }

    public boolean authenticate(final ServerHttpRequest request) {
        try {
            String host = request.getHeaders().get(HEADER_RM_HOST).get(0);
            String token = request.getHeaders().get(HEADER_RM_TOKEN).get(0);
            String initVec = request.getHeaders().get(HEADER_RM_INITIALIZATION_VECTOR).get(0);
            checkArgument(host != null && !host.trim().isBlank(), "Host header not found");
            checkArgument(token != null && !token.trim().isBlank(), "Token header not found");
            checkArgument(initVec != null && !initVec.trim().isBlank(), "Initialization Vector header not found");
            String[] decryptedTokens = defaultDecrypt(token, resourceManagerKeySpec(), initVec).split("~");
            checkArgument(decryptedTokens.length <= 3, "Invalid token found ! Missing key details");
            checkArgument(host.equals(decryptedTokens[0]), "Invalid token found ! host entry missing");
            checkArgument(decryptedTokens[1].equals(pipelineId()), "Invalid token found! pipelineId missing");
            long currentTime = System.currentTimeMillis();
            long requesterTime = Long.parseLong(decryptedTokens[2]);
            checkArgument(requesterTime < currentTime && requesterTime >= (currentTime - tokenValidity), "Invalid token found! expired token");
            return true;
        } catch (Exception e) {
            throw new SecurityException(e);
        }
    }

    public Mono<Object> saveState(final String stateId, final Object state, final ServerHttpRequest request) {
        return Mono.create(sink -> {
            try {
                authenticate(request);
                stateMap.put(stateId, state);
                sink.success(stateMap.get(stateId));
            } catch (Exception e) {
                sink.error(e);
            }
        });
    }

    public Mono<Object> getState(final String stateId, final ServerHttpRequest request) {
        return Mono.create(sink -> {
            try {
                authenticate(request);
                sink.success(stateMap.get(stateId));
            } catch (Exception e) {
                sink.error(e);
            }
        });
    }

    public Mono<Map> saveSchema(final String schemaId, final Object input, final ServerHttpRequest request) {
        return Mono.create(sink -> {
            try {
                authenticate(request);
                Schema saved = schemaMap.computeIfAbsent(schemaId, s -> {
                    String schemaString;
                    if (input instanceof String) {
                        schemaString = (String) input;
                    } else {
                        try {
                            schemaString = jsonString(input);
                        } catch (Exception e) {
                            schemaString = input.toString();
                        }
                    }
                    Schema schema = new Schema.Parser().parse(schemaString);
                    LOGGER.debug("Local schema saved successfully!!", Map.of("schema", schema.toString(), "schemaId", schemaId));
                    return schema;
                });
                sink.success(readJsonMap(saved.toString()));
            } catch (Exception e) {
                sink.error(e);
            }
        });
    }

    public Mono<Map> getSchema(final String schemaId, final ServerHttpRequest request) {
        return Mono.create(sink -> {
            try {
                authenticate(request);
                Schema schema = schemaMap.get(schemaId);
                if (schema == null && schemaId.contains(SCHEMA_DELIMITER)) {
                    schema = schemaMap.get(schemaId.split(SCHEMA_DELIMITER)[0]);
                }
                if (schema == null) {
                    sink.error(new Exception(String.format("Schema not found in coordinator cache for schemaId - %s", schemaId)));
                    return;
                }
                sink.success(readJsonMap(schema.toString()));
            } catch (Exception e) {
                sink.error(e);
            }
        });
    }

    public Mono<String> metrics() {
        return prometheusService.metrics().map(str -> asList(str, grpcPools().stream().map(pool -> {
            ExternalFunctionProtoServiceBlockingStub grpcClient = null;
            try {
                grpcClient = pool.borrowObject();
                return grpcClient.metrics(Epoc.newBuilder().setTime(System.currentTimeMillis()).build()).getResponse();
            } catch (Exception e) {
                LOGGER.warn("Could not fetch metrics from the grpc container", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
                return EMPTY_STRING;
            } finally {
                returnGrpcToPool(pool, grpcClient);
            }
        }).collect(Collectors.joining("\n"))).stream().collect(Collectors.joining("\n")));
    }

    public Mono<String> pipelineStatus(final ServerHttpRequest request) {
        return Mono.just(PIPELINE_STATUS.get());
    }
}
