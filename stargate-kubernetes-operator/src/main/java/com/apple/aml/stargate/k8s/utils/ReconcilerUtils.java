package com.apple.aml.stargate.k8s.utils;

import com.apple.aml.stargate.common.configs.VersionInfo;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.options.DeploymentOptions;
import com.apple.aml.stargate.common.pojo.CoreOptions;
import com.apple.aml.stargate.common.pojo.CoreResourceOptions;
import com.apple.aml.stargate.common.utils.AppConfig;
import com.apple.aml.stargate.common.utils.K8sUtils;
import com.apple.aml.stargate.common.utils.WebUtils;
import com.apple.aml.stargate.k8s.crd.CoreResource;
import com.apple.aml.stargate.k8s.crd.CoreStatus;
import com.apple.aml.stargate.k8s.reconciler.CoreReconciler;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.difflib.DiffUtils;
import com.github.difflib.patch.Patch;
import com.typesafe.config.ConfigFactory;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.ServiceSpec;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentSpec;
import io.fabric8.kubernetes.api.model.apps.DeploymentSpecBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSetSpec;
import io.fabric8.kubernetes.api.model.apps.StatefulSetSpecBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.JobSpec;
import io.fabric8.kubernetes.api.model.batch.v1.JobSpecBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyEgressRule;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyIngressRule;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicySpec;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.cache2k.event.CacheEntryExpiredListener;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.common.constants.CommonConstants.CONFIG_PARSE_OPTIONS_JSON;
import static com.apple.aml.stargate.common.constants.CommonConstants.CONFIG_RENDER_OPTIONS_CONCISE;
import static com.apple.aml.stargate.common.constants.CommonConstants.IDMS_APP_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sLabelValues.COORDINATOR;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sLabelValues.WORKER;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sLabels.MODE;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sLabels.PLATFORM;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sLabels.RESOURCE;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sLabels.RUNNER;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sLabels.RUN_NO;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sLabels.STARGATE_VERSION;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sLabels.VERSION_NO;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sLabels.WORKFLOW;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.STARGATE;
import static com.apple.aml.stargate.common.constants.PipelineConstants.PIPELINE_ID;
import static com.apple.aml.stargate.common.utils.AppConfig.environment;
import static com.apple.aml.stargate.common.utils.HoconUtils.hoconToPojo;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.nonNullValueMap;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJson;
import static com.apple.aml.stargate.common.utils.JsonUtils.yamlString;
import static com.apple.aml.stargate.common.utils.K8sUtils.EXECUTOR_VERSION;
import static com.apple.aml.stargate.common.utils.K8sUtils.STARGATE_IMAGE_VERSION;
import static com.apple.aml.stargate.common.utils.K8sUtils.defaultAnnotations;
import static com.apple.aml.stargate.common.utils.K8sUtils.fillDefaultLabels;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public final class ReconcilerUtils {

    public static final Cache<String, Object> SERVER_CACHE = new Cache2kBuilder<String, Object>() {

    }.entryCapacity(10).expireAfterWrite(1, TimeUnit.MINUTES).addAsyncListener((CacheEntryExpiredListener<String, Object>) (cache, entry) -> {
        Logger logger = logger(CoreReconciler.class);
        try {
            logger.info("Cache entry about to expire. Will try to fetch latest value from server", Map.of("keyName", entry.getKey()));
            Object value = getServerValue(entry.getKey());
            cache.put(entry.getKey(), value);
            logger.info("Updated cache with latest value from server", Map.of("keyName", entry.getKey()));
        } catch (Exception e) {
            logger.warn("Error in fetching latest value from server ! Will retain original", Map.of("keyName", entry.getKey(), ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
            cache.put(entry.getKey(), entry.getValue());
        }
    }).loader(key -> {
        Logger logger = logger(CoreReconciler.class);
        logger.info("Trying to fetch value from server", Map.of("keyName", key));
        try {
            Object value = getServerValue(key);
            logger.info("Was able to successfully fetch value from server", Map.of("keyName", key));
            return value;
        } catch (Exception e) {
            logger.error("Error in fetching value from server. Will return null", Map.of("keyName", key, ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
            return null;
        }
    }).build();

    private ReconcilerUtils() {
    }

    public static Object getServerValue(final String keyName) throws Exception {
        if (EXECUTOR_VERSION.equalsIgnoreCase(keyName)) {
            VersionInfo info = WebUtils.httpGet(String.format("%s/version", environment().getConfig().getStargateUri()), Map.of(), VersionInfo.class, true, false);
            if (info != null) System.setProperty(STARGATE_IMAGE_VERSION, info.getVersion());
            return info;
        }
        return keyName;
    }

    public static Map<String, String> defaultLabels(final String type, final CoreOptions options, final CoreResource resource) {
        Map<String, String> resourceLabels = resource.getMetadata().getLabels();
        Map<String, String> labels = resourceLabels == null ? new HashMap<>() : new HashMap<>(resourceLabels);
        fillDefaultLabels(type, options, labels);
        CoreStatus status = resource.getStatus();
        labels.put(PLATFORM, STARGATE);
        labels.put(K8sUtils.STARGATE_MANAGED, "true");
        labels.put(PIPELINE_ID, status.getPipelineId());
        labels.put(IDMS_APP_ID, String.valueOf(status.getAppId()));
        labels.put(MODE, status.getMode());
        labels.put(WORKFLOW, String.format("%s-%s", STARGATE, status.getPipelineId()));
        labels.put(RUNNER, status.getRunner());
        labels.put(RUN_NO, String.valueOf(status.getRunNo()));
        labels.put(VERSION_NO, String.valueOf(status.getVersionNo()));
        labels.put(STARGATE_VERSION, status.getStargateVersion());
        return labels;
    }

    public static Map<String, String> workerStatus(final Deployment deployment) {
        Map<String, String> map = new HashMap<>();
        map.put("name", deployment.getMetadata().getName());
        map.put("creationTimestamp", deployment.getMetadata().getCreationTimestamp());
        map.put("resourceVersion", deployment.getMetadata().getResourceVersion());
        map.put("availableReplicas", String.valueOf(deployment.getStatus().getAvailableReplicas()));
        map.put("readyReplicas", String.valueOf(deployment.getStatus().getReadyReplicas()));
        return map;
    }

    public static Map<String, String> coordinatorStatus(final StatefulSet statefulSet) {
        Map<String, String> map = new HashMap<>();
        map.put("name", statefulSet.getMetadata().getName());
        map.put("creationTimestamp", statefulSet.getMetadata().getCreationTimestamp());
        map.put("resourceVersion", statefulSet.getMetadata().getResourceVersion());
        map.put("availableReplicas", String.valueOf(statefulSet.getStatus().getAvailableReplicas()));
        map.put("readyReplicas", String.valueOf(statefulSet.getStatus().getReadyReplicas()));
        return map;
    }

    public static Map<String, String> coordinatorStatus(final Job job) {
        Map<String, String> map = new HashMap<>();
        map.put("name", job.getMetadata().getName());
        map.put("creationTimestamp", job.getMetadata().getCreationTimestamp());
        map.put("resourceVersion", job.getMetadata().getResourceVersion());
        map.put("startTime", String.valueOf(job.getStatus().getStartTime()));
        map.put("completionTime", String.valueOf(job.getStatus().getCompletionTime()));
        map.put("succeeded", String.valueOf(job.getStatus().getSucceeded()));
        map.put("active", String.valueOf(job.getStatus().getActive()));
        map.put("failed", String.valueOf(job.getStatus().getFailed()));
        return map;
    }

    public static Map<String, String> serviceStatus(final Service service) {
        Map<String, String> map = new HashMap<>();
        map.put("name", service.getMetadata().getName());
        map.put("creationTimestamp", service.getMetadata().getCreationTimestamp());
        map.put("resourceVersion", service.getMetadata().getResourceVersion());
        map.put("clusterIP", service.getSpec().getClusterIP());
        map.put("ports", String.valueOf(service.getSpec().getPorts().stream().map(p -> String.format("%s:%d", p.getName(), p.getPort())).collect(Collectors.toList())));
        return map;
    }

    public static Map<String, String> networkPolicyStatus(final NetworkPolicy networkPolicy) {
        Map<String, String> map = new HashMap<>();
        map.put("name", networkPolicy.getMetadata().getName());
        map.put("creationTimestamp", networkPolicy.getMetadata().getCreationTimestamp());
        map.put("resourceVersion", networkPolicy.getMetadata().getResourceVersion());
        return map;
    }

    public static Map<String, String> configMapStatus(final ConfigMap configMap) {
        Map<String, String> map = new HashMap<>();
        map.put("name", configMap.getMetadata().getName());
        map.put("creationTimestamp", configMap.getMetadata().getCreationTimestamp());
        map.put("resourceVersion", configMap.getMetadata().getResourceVersion());
        map.put("items", configMap.getData().keySet().stream().collect(Collectors.joining(",")));
        return map;
    }

    public static VersionInfo getServerVersion() {
        return (VersionInfo) SERVER_CACHE.get(EXECUTOR_VERSION);
    }

    public static void updateServerVersion(final VersionInfo versionInfo) {
        SERVER_CACHE.put(EXECUTOR_VERSION, versionInfo);
    }

    public static CoreOptions getOperatorOptions(final CoreResource resource) throws JsonProcessingException {
        CoreOptions iOptions = resource.getSpec();
        if (isBlank(iOptions.getRunner())) iOptions.setRunner(PipelineConstants.RUNNER.flink.name());
        StringBuilder builder = new StringBuilder();
        builder.append(AppConfig.config().getConfig(String.format("stargate.deployment.size.%s", iOptions.deploymentSize().name())).root().render(CONFIG_RENDER_OPTIONS_CONCISE)).append("\n\n");
        builder.append(ConfigFactory.parseString(jsonString(nonNullValueMap(iOptions)), CONFIG_PARSE_OPTIONS_JSON).root().render(CONFIG_RENDER_OPTIONS_CONCISE)).append("\n\n");
        if (!isBlank(iOptions.getMerge())) builder.append(iOptions.getMerge()).append("\n\n");
        CoreOptions options = readJson(jsonString(hoconToPojo(builder.toString(), DeploymentOptions.class)), CoreOptions.class);
        if (options.getRunnerConfigs() == null) options.setRunnerConfigs(new HashMap<>());
        return options;
    }

    public static NetworkPolicy getNetworkPolicy(final CoreResource resource, final String pipelineId, final Map<String, String> defaultLabels) {
        Map<String, String> labels = new HashMap<>(defaultLabels);
        labels.put(RESOURCE, "network-policy");
        NetworkPolicySpec spec = new NetworkPolicySpec();
        spec.setIngress(singletonList(new NetworkPolicyIngressRule()));
        spec.setEgress(singletonList(new NetworkPolicyEgressRule()));
        spec.setPolicyTypes(asList("Ingress", "Egress"));
        spec.setPodSelector(new LabelSelectorBuilder().addToMatchLabels(Map.of(PIPELINE_ID, pipelineId)).build());
        var policy = new NetworkPolicyBuilder().withNewMetadata().withName(networkPolicyName(pipelineId)).addToLabels(labels).withNamespace(resource.getMetadata().getNamespace()).endMetadata().withSpec(spec).build();
        policy.addOwnerReference(resource);
        return policy;
    }

    public static String networkPolicyName(final String pipelineId) {
        return String.format("stargate-%s", pipelineId);
    }

    public static ConfigMap getConfigMap(final CoreResource resource, final String pipelineId, final Map<String, String> defaultLabels, final Map<String, String> configData) {
        Map<String, String> labels = new HashMap<>(defaultLabels);
        labels.put(RESOURCE, "config-map");
        Map<String, String> cm = new HashMap<>();
        configData.forEach((k, v) -> cm.put(K8sUtils.configMapKey(k), v));
        var configMap = new ConfigMapBuilder().withNewMetadata().withName(K8sUtils.configMapName(pipelineId)).addToLabels(labels).withNamespace(resource.getMetadata().getNamespace()).endMetadata().withData(cm).build();
        configMap.addOwnerReference(resource);
        return configMap;
    }

    public static Service getCoordinatorService(final CoreResource resource, final String pipelineId, final Map<String, String> defaultLabels) {
        Map<String, String> labels = new HashMap<>(defaultLabels);
        labels.put(RESOURCE, String.format("%s-service", COORDINATOR));
        Map<String, Integer> ports = new HashMap<>();
        resource.getSpec().runner().getConfig().getCoordinator().ports().forEach((k, v) -> ports.put(K8sUtils.simplePortName(k), v));
        ServiceSpec spec = new ServiceSpec();
        spec.setType("ClusterIP");
        spec.setSelector(Map.of(PIPELINE_ID, pipelineId, RESOURCE, COORDINATOR));
        spec.setPorts(ports.entrySet().stream().map(e -> {
            ServicePort servicePort = new ServicePort();
            servicePort.setName(e.getKey());
            servicePort.setPort(e.getValue());
            return servicePort;
        }).collect(Collectors.toList()));
        var service = new ServiceBuilder().withNewMetadata().withName(K8sUtils.coordinatorName(pipelineId)).addToLabels(labels).withNamespace(resource.getMetadata().getNamespace()).endMetadata().withSpec(spec).build();
        service.addOwnerReference(resource);
        return service;
    }

    public static Deployment getWorkerDeployment(final CoreResource resource, final String pipelineId, final long runNo, final Map<String, String> defaultLabels, final CoreOptions options, final String sharedToken) throws Exception {
        CoreResourceOptions worker = options.worker();
        Map<String, String> labels = new HashMap<>(defaultLabels);
        labels.put(RESOURCE, WORKER);
        labels.putAll(options.runner().getConfig().getWorker().labels());
        Map<String, String> annotations = defaultAnnotations(WORKER, options);
        if (resource.getMetadata().getAnnotations() != null) annotations.putAll(resource.getMetadata().getAnnotations());
        PodSpec podSpec = K8sUtils.getPodSpec(WORKER, pipelineId, runNo, options, sharedToken);
        PodTemplateSpec templateSpec = new PodTemplateSpecBuilder().withNewMetadata().addToLabels(labels).addToAnnotations(annotations).endMetadata().withSpec(podSpec).build();
        DeploymentSpec spec = new DeploymentSpecBuilder().withReplicas(worker.getReplicas()).withSelector(new LabelSelectorBuilder().addToMatchLabels(Map.of(PIPELINE_ID, pipelineId, RESOURCE, WORKER, RUN_NO, String.valueOf(runNo))).build()).withTemplate(templateSpec).build();
        var deployment = new DeploymentBuilder().withNewMetadata().withName(workerName(pipelineId)).addToLabels(labels).withNamespace(resource.getMetadata().getNamespace()).endMetadata().withSpec(spec).build();
        deployment.addOwnerReference(resource);
        return deployment;
    }

    public static String workerName(final String pipelineId) {
        return String.format("%s-%s", pipelineId, WORKER);
    }

    public static StatefulSet getCoordinatorStatefulSet(final CoreResource resource, final String pipelineId, final long runNo, final Map<String, String> defaultLabels, final CoreOptions options, final String sharedToken) throws Exception {
        Map<String, String> labels = new HashMap<>(defaultLabels);
        labels.put(RESOURCE, COORDINATOR);
        labels.putAll(options.runner().getConfig().getCoordinator().labels());
        Map<String, String> annotations = defaultAnnotations(COORDINATOR, options);
        if (resource.getMetadata().getAnnotations() != null) annotations.putAll(resource.getMetadata().getAnnotations());
        PodSpec podSpec = K8sUtils.getPodSpec(COORDINATOR, pipelineId, runNo, options, sharedToken);
        PodTemplateSpec templateSpec = new PodTemplateSpecBuilder().withNewMetadata().addToLabels(labels).addToAnnotations(annotations).endMetadata().withSpec(podSpec).build();
        StatefulSetSpec spec = new StatefulSetSpecBuilder().withReplicas(1).withSelector(new LabelSelectorBuilder().addToMatchLabels(Map.of(PIPELINE_ID, pipelineId, RESOURCE, COORDINATOR, RUN_NO, String.valueOf(runNo))).build()).withTemplate(templateSpec).withServiceName(K8sUtils.coordinatorName(pipelineId)).build();
        var statefulSet = new StatefulSetBuilder().withNewMetadata().withName(K8sUtils.coordinatorName(pipelineId)).addToLabels(labels).withNamespace(resource.getMetadata().getNamespace()).endMetadata().withSpec(spec).build();
        statefulSet.addOwnerReference(resource);
        return statefulSet;
    }

    /**
     * The getCoordinatorJob function creates a Kubernetes Job object for the coordinator.
     * The function takes in the following parameters:
     *
     *
     * @param final CoreResource resource Add the owner reference to the job
     * @param final String pipelineId Set the name of the job
     * @param final long runNo Create a unique name for the job
     * @param final Map&lt;String Add labels to the job
     * @param String&gt; defaultLabels Set the labels for the job
     * @param final CoreOptions options Get the podspec
    public static podspec getpodspec(final string resource, final string pipelineid, final long runno, final coreoptions options, final string sharedtoken) {
            map&lt;string, quantity&gt; requests = new hashmap&lt;&gt;(options
     * @param final String sharedToken Pass the shared token to the pod
     *
     * @return A job object
     *
     */
    public static Job getCoordinatorJob(final CoreResource resource, final String pipelineId, final long runNo, final Map<String, String> defaultLabels, final CoreOptions options, final String sharedToken) throws Exception {
        Map<String, String> labels = new HashMap<>(defaultLabels);
        labels.put(RESOURCE, COORDINATOR);
        labels.putAll(options.runner().getConfig().getCoordinator().labels());
        Map<String, String> annotations = defaultAnnotations(COORDINATOR, options);
        if (resource.getMetadata().getAnnotations() != null) annotations.putAll(resource.getMetadata().getAnnotations());
        PodSpec podSpec = K8sUtils.getPodSpec(COORDINATOR, pipelineId, runNo, options, sharedToken);
        PodTemplateSpec templateSpec = new PodTemplateSpecBuilder().withNewMetadata().addToLabels(labels).addToAnnotations(annotations).endMetadata().withSpec(podSpec).build();
        JobSpec spec = new JobSpecBuilder().withTemplate(templateSpec).withParallelism(1).withBackoffLimit(Math.max(1, options.backoffLimit())).build();
        var job = new JobBuilder().withNewMetadata().withName(K8sUtils.coordinatorName(pipelineId)).addToLabels(labels).withNamespace(resource.getMetadata().getNamespace()).endMetadata().withSpec(spec).build();
        job.addOwnerReference(resource);
        return job;
    }

    public static String getK8sManifestDiff(final Object obj1, final Object obj2) {
        try {
            List<String> yaml1 = List.of(yamlString(obj1).split("\n"));
            List<String> yaml2 = List.of(yamlString(obj2).split("\n"));
            Patch<String> patch = DiffUtils.diff(yaml1, yaml2);
            return patch.getDeltas().stream().map(d -> d.toString()).collect(Collectors.joining("\n"));
        } catch (Exception e) {
            return null;
        }
    }

}
