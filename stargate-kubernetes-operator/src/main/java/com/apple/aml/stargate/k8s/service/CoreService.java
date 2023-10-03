package com.apple.aml.stargate.k8s.service;

import com.apple.aml.stargate.common.configs.VersionInfo;
import com.apple.aml.stargate.common.exceptions.GenericException;
import com.apple.aml.stargate.common.exceptions.UnauthorizedException;
import com.apple.aml.stargate.common.utils.A3Utils;
import com.apple.aml.stargate.common.utils.WebUtils;
import com.apple.aml.stargate.common.web.clients.AppConfigClient;
import com.apple.aml.stargate.k8s.reconciler.CoreReconciler;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.Operator;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import javax.annotation.PostConstruct;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.A3Constants.KNOWN_APP.STARGATE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.PipelineConstants.DirectoryPaths.CONNECT_ID_PROP_FILE_NAME;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.APPCONFIG_ENVIRONMENT;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.APPCONFIG_MODULE_ID;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_CONNECT_BACKEND;
import static com.apple.aml.stargate.common.constants.PipelineConstants.PIPELINE_ID;
import static com.apple.aml.stargate.common.utils.A3Utils.a3Mode;
import static com.apple.aml.stargate.common.utils.A3Utils.getA3Token;
import static com.apple.aml.stargate.common.utils.AppConfig.appId;
import static com.apple.aml.stargate.common.utils.AppConfig.env;
import static com.apple.aml.stargate.common.utils.AppConfig.environment;
import static com.apple.aml.stargate.common.utils.ClassUtils.parseBoolean;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;
import static com.apple.aml.stargate.common.utils.K8sUtils.STARGATE_MANAGED;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.k8s.utils.ReconcilerUtils.getServerVersion;
import static com.apple.aml.stargate.k8s.utils.ReconcilerUtils.updateServerVersion;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Base64.getDecoder;
import static java.util.Base64.getEncoder;

@Service
public class CoreService {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    @Autowired
    private KubernetesClient client;
    @Autowired
    private CoreReconciler reconciler;
    private Operator operator;
    private String namespace;

    @PostConstruct
    void init() {
        operator = new Operator(client, o -> o.withStopOnInformerErrorDuringStartup(false));
        operator.register(reconciler);
        operator.start();
        this.namespace = env("APP_NAMESPACE", "stargate-system");
    }

    public Mono<io.fabric8.kubernetes.client.VersionInfo> k8sVersionInfo() {
        return Mono.just(client.getKubernetesVersion());
    }

    public Mono<String> readiness() {
        return Mono.create(sink -> {
            try {
                VersionInfo versionInfo = WebUtils.httpGet(String.format("%s/version", environment().getConfig().getStargateUri()), Map.of(), VersionInfo.class, true, false);
                if (versionInfo == null || isBlank(versionInfo.getTag())) {
                    sink.error(new GenericException("Invalid versionInfo from server!!"));
                    return;
                }
                updateServerVersion(versionInfo);
                sink.success("OK");
            } catch (Exception e) {
                sink.error(e);
            }
        });
    }

    public Mono<String> liveness() {
        return Mono.create(sink -> {
            try {
                VersionInfo versionInfo = getServerVersion();
                if (versionInfo == null || isBlank(versionInfo.getTag())) {
                    sink.error(new GenericException("Invalid versionInfo from cache!!"));
                    return;
                }
                sink.success("OK");
            } catch (Exception e) {
                sink.error(e);
            }
        });
    }

    @SuppressWarnings("unchecked")
    public Mono<Map<String, String>> getPipelineProperties(final Long appId, final String pipelineId, final String token, final ServerHttpRequest request) {
        return Mono.create(sink -> {
            try {
                validateToken(appId, pipelineId, token, request);
                String stargateUri = environment().getConfig().getStargateUri();
                String a3Token = getA3Token(STARGATE.appId());
                Map<String, String> properties = WebUtils.getJsonData(stargateUri + "/sg/pipeline/props/" + pipelineId, appId(), a3Token, a3Mode(), Map.class);
                sink.success(properties);
            } catch (Exception e) {
                LOGGER.error("Failed to fetch pipeline properties", Map.of(PIPELINE_ID, pipelineId, ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
                sink.error(e);
            }
        });
    }

    private void validateToken(final Long appId, final String pipelineId, final String token, final ServerHttpRequest request) throws SecurityException {
        if (appId != appId()) throw new SecurityException("AppID supplied is not allowed/configured in operator!!");
        String sharedToken = reconciler.sharedToken(pipelineId);
        if (isBlank(sharedToken)) throw new IllegalArgumentException(String.format("There is no pipeline deployed with name : %s using appId : %d", pipelineId, appId));
        if (!sharedToken.equals(token)) throw new SecurityException("Invalid token!!");
    }

    @SuppressWarnings("unchecked")
    public Mono<String> getPipelineSpec(final Long appId, final String pipelineId, final String token, final ServerHttpRequest request) {
        return Mono.create(sink -> {
            try {
                validateToken(appId, pipelineId, token, request);
                String stargateUri = environment().getConfig().getStargateUri();
                String a3Token = getA3Token(STARGATE.appId());
                String spec = WebUtils.getJsonData(stargateUri + "/sg/pipeline/spec/" + pipelineId, appId(), a3Token, a3Mode(), String.class);
                sink.success(spec);
            } catch (Exception e) {
                LOGGER.error("Failed to fetch pipeline spec", Map.of(PIPELINE_ID, pipelineId, ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
                sink.error(e);
            }
        });
    }

    @SuppressWarnings("unchecked")
    public Mono<Map<String, String>> getConnectInfo(final Long appId, final String pipelineId, final String token, final String connectId, final ServerHttpRequest request) {
        return Mono.create(sink -> {
            try {
                validateToken(appId, pipelineId, token, request);
                final Map<String, String> connectInfo = fetchLocalConnectInfo(connectId);
                if (!connectInfo.isEmpty()) {
                    sink.success(connectInfo);
                    return;
                }

                if ("appconfig".equalsIgnoreCase(env(STARGATE_CONNECT_BACKEND, null))) {
                    sink.success(AppConfigClient.getProperties(connectId, env(APPCONFIG_MODULE_ID, null), env(APPCONFIG_ENVIRONMENT, null)));
                    return;
                }

                String stargateUri = environment().getConfig().getStargateUri();
                String a3Token = getA3Token(STARGATE.appId());
                sink.success(WebUtils.getJsonData(stargateUri + "/sg/pipeline/connect-info/" + pipelineId + "/" + connectId, appId(), a3Token, a3Mode(), Map.class));
            } catch (Exception e) {
                LOGGER.error("Failed to fetch connect info associate with pipeline", Map.of(PIPELINE_ID, pipelineId, "connectId", connectId, ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
                sink.error(e);
            }
        });
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> fetchLocalConnectInfo(final String connectId) throws JsonProcessingException {
        final Map<String, String> connectInfo = new HashMap<>();
        String resourceName = connectId.replace('_', '-').toLowerCase();
        ConfigMap configMap = client.configMaps().inNamespace(namespace).withName(resourceName).get();
        Map<String, String> labels = configMap == null ? null : (configMap.getMetadata() == null ? null : configMap.getMetadata().getLabels());
        if (labels != null && parseBoolean(labels.get(STARGATE_MANAGED)) && configMap.getData() != null) configMap.getData().forEach((k, v) -> connectInfo.put(k, getEncoder().encodeToString(v.getBytes(UTF_8))));
        Secret secret = client.secrets().inNamespace(namespace).withName(resourceName).get();
        labels = secret == null ? null : (secret.getMetadata() == null ? null : secret.getMetadata().getLabels());
        if (labels != null && parseBoolean(labels.get(STARGATE_MANAGED)) && secret.getData() != null) {
            Map<String, String> secretInfo = secret.getData();
            if (secretInfo.containsKey(CONNECT_ID_PROP_FILE_NAME) && connectInfo.containsKey(CONNECT_ID_PROP_FILE_NAME)) {
                Map<String, Object> props = (Map) readJsonMap(new String(getDecoder().decode(connectInfo.remove(CONNECT_ID_PROP_FILE_NAME)), UTF_8));
                props.putAll((Map) readJsonMap(new String(getDecoder().decode(secretInfo.remove(CONNECT_ID_PROP_FILE_NAME)), UTF_8)));
                connectInfo.put(CONNECT_ID_PROP_FILE_NAME, getEncoder().encodeToString(jsonString(props).getBytes(UTF_8)));
            }
            connectInfo.putAll(secretInfo);
        }
        return connectInfo;
    }

    @SuppressWarnings("deprecation")
    public Mono<Map<String, Object>> saveConnectInfoLocallyAsSecret(final String token, final String connectId, final Map<String, Object> connectInfo, final ServerHttpRequest request) {
        return Mono.create(sink -> {
            try {
                validateA3(token);
                if (connectInfo == null || connectInfo.isEmpty()) throw new IllegalArgumentException("Empty connectInfo supplied");
                String resourceName = connectId.replace('_', '-').toLowerCase();
                Secret secret = client.secrets().inNamespace(namespace).withName(resourceName).get();
                if (secret != null) {
                    LOGGER.info("ConnectId already exists in k8s secret. New one will replace it", Map.of("connectId", connectId));
                }
                Map<String, String> map = updatedConnectInfo(connectInfo);
                Secret newSecret = new SecretBuilder().withNewMetadata().withName(resourceName).withNamespace(namespace).withLabels(Map.of(STARGATE_MANAGED, "true")).endMetadata().addToData(map).build();
                newSecret = client.secrets().resource(newSecret).createOrReplace();
                if (newSecret == null) throw new Exception("Could not create/update the connectInfo k8s secret");
                sink.success(Map.of("status", "connectId as secret created/updated successfully", "connectId", connectId));
            } catch (Exception e) {
                LOGGER.error("Failed to save connect info locally on the cluster", Map.of("connectId", connectId, ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
                sink.error(e);
            }
        });
    }

    private void validateA3(final String token) throws UnauthorizedException {
        try {
            if (!A3Utils.validate(appId(), token)) throw new UnauthorizedException("Invalid token");
        } catch (Exception e) {
            throw new UnauthorizedException("Could not authenticate", e);
        }
    }

    private static Map<String, String> updatedConnectInfo(final Map<String, Object> connectInfo) throws JsonProcessingException {
        Map<String, String> map = new HashMap<>(connectInfo.size());
        for (Map.Entry<String, Object> entry : connectInfo.entrySet()) {
            Object value = entry.getValue();
            if (value == null) continue;
            if (value instanceof Map) {
                map.put(entry.getKey(), getEncoder().encodeToString(jsonString(value).getBytes(UTF_8)));
                continue;
            }
            if (value instanceof String) {
                try {
                    byte[] bytes = getDecoder().decode((String) value);
                    if (bytes == null || bytes.length == 0) throw new Exception("Not a valid encoded string");
                    map.put(entry.getKey(), (String) value);
                } catch (Exception e) {
                    map.put(entry.getKey(), getEncoder().encodeToString(((String) value).getBytes(UTF_8)));
                }
                continue;
            }
            map.put(entry.getKey(), getEncoder().encodeToString(String.valueOf(value).getBytes(UTF_8)));
        }
        return map;
    }

    @SuppressWarnings("deprecation")
    public Mono<Map<String, Object>> saveConnectInfoLocallyAsConfigMap(final String token, final String connectId, final Map<String, Object> connectInfo, final ServerHttpRequest request) {
        return Mono.create(sink -> {
            try {
                validateA3(token);
                if (connectInfo == null || connectInfo.isEmpty()) throw new IllegalArgumentException("Empty connectInfo supplied");
                String resourceName = connectId.replace('_', '-').toLowerCase();
                ConfigMap configMap = client.configMaps().inNamespace(namespace).withName(resourceName).get();
                if (configMap != null) {
                    LOGGER.info("ConnectId already exists in k8s configMap. New one will replace it", Map.of("connectId", connectId));
                }
                Map<String, String> map = new HashMap<>(connectInfo.size());
                connectInfo.forEach((k, v) -> map.put(k, v instanceof String ? (String) v : (jsonString(v))));
                ConfigMap newConfigMap = new ConfigMapBuilder().withNewMetadata().withName(resourceName).withNamespace(namespace).withLabels(Map.of(STARGATE_MANAGED, "true")).endMetadata().addToData(map).build();
                newConfigMap = client.configMaps().resource(newConfigMap).createOrReplace();
                if (newConfigMap == null) throw new Exception("Could not create/update the connectInfo k8s configMap");
                sink.success(Map.of("status", "connectId as configMap created/updated successfully", "connectId", connectId));
            } catch (Exception e) {
                LOGGER.error("Failed to save connect info locally on the cluster", Map.of("connectId", connectId, ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
                sink.error(e);
            }
        });
    }

    public Mono<Map<String, String>> getLocalConnectInfo(final String token, final String connectId, final ServerHttpRequest request) {
        return Mono.create(sink -> {
            try {
                validateA3(token);
                sink.success(fetchLocalConnectInfo(connectId));
            } catch (Exception e) {
                LOGGER.error("Failed to save connect info locally on the cluster", Map.of("connectId", connectId, ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
                sink.error(e);
            }
        });
    }
}
