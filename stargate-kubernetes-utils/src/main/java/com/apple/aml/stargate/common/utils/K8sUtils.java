package com.apple.aml.stargate.common.utils;

import com.apple.aml.stargate.common.configs.RunnerConfig;
import com.apple.aml.stargate.common.configs.RunnerResourceConfig;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.pojo.CoreOptions;
import com.apple.aml.stargate.common.pojo.CoreResourceOptions;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.DownwardAPIVolumeFileBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.EnvVarSourceBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LifecycleBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;

import java.lang.module.ModuleDescriptor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.common.constants.A3Constants.APP_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.EMPTY_STRING;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sLabelValues.COORDINATOR;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sLabelValues.WORKER;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sLabels.PLATFORM;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sLabels.RESOURCE;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sLabels.RUN_NO;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sLabels.STARGATE_VERSION;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sPorts.APM;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sPorts.DEBUGGER;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sPorts.GRPC;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sPorts.RM;
import static com.apple.aml.stargate.common.constants.CommonConstants.SIZE_CONVERSION_BASE2_TO_H;
import static com.apple.aml.stargate.common.constants.CommonConstants.STARGATE;
import static com.apple.aml.stargate.common.constants.PipelineConstants.DEPLOYMENT_TARGET.stargate;
import static com.apple.aml.stargate.common.constants.PipelineConstants.DirectoryPaths.EXTERNAL_APP_LIB;
import static com.apple.aml.stargate.common.constants.PipelineConstants.DirectoryPaths.FLINK_CONF_FILE;
import static com.apple.aml.stargate.common.constants.PipelineConstants.DirectoryPaths.STARGATE_LOGS;
import static com.apple.aml.stargate.common.constants.PipelineConstants.DirectoryPaths.STARGATE_RUNTIME_HOME;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EXTERNAL_FUNC.generic;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EXTERNAL_FUNC.jvm;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EXTERNAL_FUNC.python;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.APP_LOG_ADDITIONAL_PREFIX;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.APP_ON_EXECUTION_ACTION;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_BASE_SHARED_DIRECTORY;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_CONTAINER_DEBUG_PORT;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_CONTAINER_GRPC_THREAD_POOL_SIZE;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_CONTAINER_PORT;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_EXTERNAL_FUNC_ENDPOINTS;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_MAIN_CONTAINER_GRPC_ENDPOINT;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_OPERATOR_URI;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_PIPELINE_ID;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_REMOTE_RM_URI;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_SHARED_TOKEN;
import static com.apple.aml.stargate.common.constants.PipelineConstants.PIPELINE_ID;
import static com.apple.aml.stargate.common.constants.PipelineConstants.VERSION_INFO;
import static com.apple.aml.stargate.common.utils.AppConfig.config;
import static com.apple.aml.stargate.common.utils.AppConfig.env;
import static com.apple.aml.stargate.common.utils.AppConfig.environment;
import static com.apple.aml.stargate.common.utils.ClassUtils.duplicate;
import static com.apple.aml.stargate.common.utils.ClassUtils.getAs;
import static com.apple.aml.stargate.common.utils.ClassUtils.parseBoolean;
import static com.apple.aml.stargate.common.utils.JsonUtils.simpleYamlString;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static java.util.Arrays.asList;

public final class K8sUtils {
    public static final String STARGATE_MANAGED = "stargate-managed";
    public static final String FLINK_MAIN_CONTAINER_NAME = "flink-main-container";
    public static final String STARGATE_IMAGE_VERSION = "APP_STARGATE_IMAGE_VERSION";
    public static final Pattern UNIT_PATTERN = Pattern.compile("([0-9]+)(.*)");
    public static final String EXECUTOR_VERSION = "EXECUTOR_VERSION";
    public static final Map<String, String> PROMETHEUS_ANNOTATIONS = Map.of("prometheus.io/scrape", "true", "prometheus.io/port", String.valueOf(RM), "prometheus.io/path", "/metrics");
    public static final String JVM_OPTION_DEBUGGER = String.format("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:%d", DEBUGGER);
    public static final String JVM_OPTION_DEBUGGER_NON_SUSPEND = String.format("-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:%d", DEBUGGER);
    public static final String JVM_OPTION_APM_GLOWROOT = "-javaagent:/opt/glowroot/glowroot.jar";
    public static final String DIRECTORY_PATH_SHARED_BASE = env(STARGATE_BASE_SHARED_DIRECTORY, "/mnt/app/shared");

    public enum STATUS {
        NOT_STARTED, INITIALIZING, RUNNING, WAITING, COMPLETED, ERROR
    }

    private K8sUtils() {
    }

    public static Pod getPodTemplate(final String type, final long runNo, final CoreOptions options, final String sharedToken, final Map<String, String> defaultLabels, Map<String, String> defaultAnnotations) throws Exception {
        Map<String, String> labels = new HashMap<>();
        labels.putAll(COORDINATOR.equals(type) ? options.runner().getConfig().getCoordinator().labels() : options.runner().getConfig().getWorker().labels());
        labels.put(RESOURCE, type);
        labels.putAll(defaultLabels);
        PodBuilder builder = new PodBuilder();
        builder.withNewMetadata().withName("pod-template").withLabels(labels).withAnnotations(defaultAnnotations).endMetadata().withSpec(getPodSpec(type, options.getPipelineId(), runNo, options, sharedToken));
        return builder.build();
    }

    /**
     * The getPodSpec function is responsible for creating a PodSpec object that will be used to create the Kubernetes pod.
     * The function takes in several parameters, including:
     *
     *
     * @param final String type Determine whether the pod is a coordinator or worker
     * @param final String pipelineId Create the name of the pod
     * @param final long runNo Generate the pod name
     * @param final CoreOptions options Get the imagepullpolicy, which is used to set the imagepullpolicy for each container
     * @param final String sharedToken Pass the shared token to the getpodspec function
     *
     * @return The following podspec object
     *
     */
    public static PodSpec getPodSpec(final String type, final String pipelineId, final long runNo, final CoreOptions options, final String sharedToken) throws Exception {
        PipelineConstants.RUNNER runner = options.runner();
        PipelineConstants.DEPLOYMENT_TARGET deploymentTarget = options.deploymentTarget();
        RunnerConfig runnerConfig = runner.getConfig();
        CoreResourceOptions resourceOptions;
        RunnerResourceConfig runnerResourceConfig;
        List<ContainerBuilder> containerBuilders = new ArrayList<>();
        String stargateImage = stargateImage(options);
        String mainImage;
        boolean initContainerRequired;
        List<String> setupArguments = new ArrayList<>();
        PodSpecBuilder podBuilder = new PodSpecBuilder();
        options.calculateParallelism();

        if (COORDINATOR.equals(type)) {
            resourceOptions = options.coordinator();
            runnerResourceConfig = runnerConfig.getCoordinator();
            mainImage = stargateImage;
            initContainerRequired = false;
        } else {
            resourceOptions = options.worker();
            runnerResourceConfig = runnerConfig.getWorker();
            mainImage = isBlank(options.getImage()) ? stargateImage : mainImage(options);
            boolean isInternalImage = mainImage.startsWith(config().getString("stargate.deployment.docker.image.executorPrefix").trim());
            initContainerRequired = !isBlank(options.getPythonImage()) || !isBlank(options.getExternalJvmImage()) || !isBlank(options.getExternalImage()) || !isInternalImage;
            if (!isInternalImage) setupArguments.add(FLINK_MAIN_CONTAINER_NAME);
        }
        String imagePullPolicy = options.imagePullPolicy();
        Map<String, Quantity> mcRequests = Map.of("cpu", new Quantity(resourceOptions.getCpu()), "memory", new Quantity(resourceOptions.getMemory()));
        Map<String, Quantity> mcLimits = Map.of("cpu", new Quantity(isBlank(resourceOptions.getCpuLimit()) ? resourceOptions.getCpu() : resourceOptions.getCpuLimit()), "memory", new Quantity(isBlank(resourceOptions.getMemoryLimit()) ? resourceOptions.getMemory() : resourceOptions.getMemoryLimit()));
        Map<String, Integer> mcPorts = new HashMap<>();
        runnerResourceConfig.ports().forEach((k, v) -> mcPorts.put(simplePortName(k), v));
        ContainerBuilder mainBuilder = new ContainerBuilder().withName(FLINK_MAIN_CONTAINER_NAME).withImage(mainImage).withImagePullPolicy(imagePullPolicy).withNewResources().addToRequests(mcRequests).addToLimits(mcLimits).endResources();
        List<String> jvmOptions = resourceOptions.getJvmOptions() == null ? new ArrayList<>() : new ArrayList<>(resourceOptions.getJvmOptions());
        Map<String, EnvVar> env = getEnvMap(options, pipelineId, runNo, runner, sharedToken, type);
        boolean enableDebugMode = parseBoolean(resourceOptions.getDebugMode()) || "silent".equalsIgnoreCase(resourceOptions.getDebugMode()) || type.equalsIgnoreCase(resourceOptions.getDebugMode());
        if (enableDebugMode) {
            jvmOptions.add("silent".equalsIgnoreCase(resourceOptions.getDebugMode()) ? JVM_OPTION_DEBUGGER_NON_SUSPEND : JVM_OPTION_DEBUGGER);
            mcPorts.put("debugger", DEBUGGER);
        }
        if (parseBoolean(resourceOptions.getApmMode()) || type.equalsIgnoreCase(resourceOptions.getApmMode())) {
            jvmOptions.add(JVM_OPTION_APM_GLOWROOT);
            mcPorts.put("apm", APM);
        }
        if (options.getEnv() != null) options.getEnv().stream().map(m -> getAs(m, EnvVar.class)).forEach(e -> env.put(e.getName(), e));
        if (options.getGlobals() != null) options.getGlobals().forEach((k, v) -> env.put(k, new EnvVarBuilder().withName(k).withValue(v).build()));
        if (resourceOptions.getEnv() != null) resourceOptions.getEnv().stream().map(m -> getAs(m, EnvVar.class)).forEach(e -> env.put(e.getName(), e));
        if (WORKER.equals(type)) {
            List<String> sidecarEndpoints = new ArrayList<>();
            env.put(STARGATE_REMOTE_RM_URI, new EnvVarBuilder().withName(STARGATE_REMOTE_RM_URI).withValue(String.format("http://%s:%d", coordinatorName(pipelineId), RM)).build());
            if (!isBlank(options.getPythonImage())) containerBuilders.add(sidecarBuilder(options, resourceOptions, "python", options.getPythonImage(), imagePullPolicy, resourceOptions.getPythonCPU(), resourceOptions.getPythonMemory(), resourceOptions.getPythonCPULimit(), resourceOptions.getPythonMemoryLimit(), python.portNo(), enableDebugMode, python.debugPortNo(), env, sidecarEndpoints, setupArguments));
            if (!isBlank(options.getExternalJvmImage())) containerBuilders.add(sidecarBuilder(options, resourceOptions, "jvm", options.getExternalJvmImage(), imagePullPolicy, resourceOptions.getExternalJvmCPU(), resourceOptions.getExternalJvmMemory(), resourceOptions.getExternalJvmCPULimit(), resourceOptions.getExternalJvmMemoryLimit(), jvm.portNo(), enableDebugMode, jvm.debugPortNo(), env, sidecarEndpoints, setupArguments));
            if (!isBlank(options.getExternalImage())) containerBuilders.add(sidecarBuilder(options, resourceOptions, "external", options.getExternalImage(), imagePullPolicy, resourceOptions.getExternalCPU(), resourceOptions.getExternalMemory(), resourceOptions.getExternalCPULimit(), resourceOptions.getExternalMemoryLimit(), generic.portNo(), enableDebugMode, generic.debugPortNo(), env, sidecarEndpoints, setupArguments));
            if (!sidecarEndpoints.isEmpty()) env.put(STARGATE_EXTERNAL_FUNC_ENDPOINTS, new EnvVarBuilder().withName(STARGATE_EXTERNAL_FUNC_ENDPOINTS).withValue(sidecarEndpoints.stream().collect(Collectors.joining(","))).build());
        } else {
            env.put("POD_IP", new EnvVarBuilder().withName("POD_IP").withNewValueFrom().withNewFieldRef("v1", "status.podIP").endValueFrom().build());
            if (deploymentTarget == stargate) env.put(APP_ON_EXECUTION_ACTION, new EnvVarBuilder().withName(APP_ON_EXECUTION_ACTION).withValue("sleep").build());
        }
        if (!jvmOptions.isEmpty()) {
            String jvmOptionsString = jvmOptions.stream().collect(Collectors.joining(" "));
            asList(String.format("FLINK_ENV_JAVA_OPTS_%s", COORDINATOR.equals(type) ? "JM" : "TM"), "STARGATE_EXECUTOR_OPTS").stream().forEach(key -> env.put(key, new EnvVarBuilder().withName(key).withValue(jvmOptionsString).build()));
        }

        mainBuilder = mainBuilder.withEnv(new ArrayList<>(env.values()));
        List<String> args = new ArrayList<>();
        args.add(String.format("/opt%s/run-executor.sh", initContainerRequired ? "/stargate-runtime" : EMPTY_STRING));
        if (!isBlank(runnerResourceConfig.getCommand())) args.add(runnerResourceConfig.getCommand());
        if (runnerResourceConfig.getArguments() != null) args.addAll(runnerResourceConfig.getArguments());
        if (COORDINATOR.equals(type)) addCoordinatorArgs(pipelineId, runNo, options, runner, args);

        mainBuilder = mainBuilder.withCommand("/bin/sh").withArgs("-c", args.stream().collect(Collectors.joining(" ")));
        mainBuilder = mainBuilder.withPorts(mcPorts.entrySet().stream().map(e -> new ContainerPortBuilder().withName(e.getKey()).withContainerPort(e.getValue()).build()).collect(Collectors.toList()));

        mainBuilder = mainBuilder.withNewLivenessProbe().withNewTcpSocket().withPort(new IntOrString(runnerResourceConfig.getLivenessPort())).endTcpSocket().withInitialDelaySeconds(30).withPeriodSeconds(60).withTimeoutSeconds(5).endLivenessProbe();

        boolean useSplunkForwarder = parseBoolean(resourceOptions.getLogForwarder()) || "default".equalsIgnoreCase(resourceOptions.getLogForwarder()) || "heavy".equalsIgnoreCase(resourceOptions.getLogForwarder());
        List<Volume> volumes = new ArrayList<>();
        if (deploymentTarget == stargate) volumes.add(new VolumeBuilder().withName("stargate-config").withNewConfigMap().withName(configMapName(pipelineId)).endConfigMap().build());
        VolumeBuilder baseDirVBuilder = new VolumeBuilder().withName("stargate-base-shared-dir");
        String pvcClaim = pvcClaim(options);
        baseDirVBuilder = isBlank(pvcClaim) ? baseDirVBuilder.withNewEmptyDir().endEmptyDir() : baseDirVBuilder.withNewPersistentVolumeClaim(pvcClaim, false);
        volumes.add(baseDirVBuilder.build());
        volumes.add(new VolumeBuilder().withName("stargate-runtime").withNewEmptyDir().endEmptyDir().build());
        volumes.add(new VolumeBuilder().withName("splunk-logs").withNewEmptyDir().endEmptyDir().build());
        volumes.add(new VolumeBuilder().withName("splunk-temp").withNewEmptyDir().endEmptyDir().build());
        volumes.add(new VolumeBuilder().withName("pod-info").withNewDownwardAPI().withDefaultMode(420).withItems(asList("labels", "uid").stream().map(x -> new DownwardAPIVolumeFileBuilder().withNewFieldRef("v1", String.format("metadata.%s", x)).withPath(x).build()).collect(Collectors.toList())).endDownwardAPI().build());

        List<VolumeMount> mcVMounts = getDefaultVolumeMounts();
        if (deploymentTarget == stargate) getConfigDataMap(pipelineId, options).keySet().forEach(k -> mcVMounts.add(new VolumeMountBuilder().withName("stargate-config").withMountPath(k).withSubPath(configMapKey(k)).withReadOnly().build()));
        mcVMounts.add(new VolumeMountBuilder().withName("stargate-base-shared-dir").withMountPath(DIRECTORY_PATH_SHARED_BASE).withReadOnly(false).build());
        mainBuilder = mainBuilder.withVolumeMounts(mcVMounts);

        containerBuilders.add(mainBuilder);

        if (deploymentTarget == stargate && useSplunkForwarder) {
            Map<String, Quantity> splunkRequests = Map.of("cpu", new Quantity(resourceOptions.getSplunkCPU()), "memory", new Quantity(resourceOptions.getSplunkMemory()));
            Map<String, Quantity> splunkLimits = Map.of("cpu", new Quantity(isBlank(resourceOptions.getSplunkCPULimit()) ? resourceOptions.getSplunkCPU() : resourceOptions.getSplunkCPULimit()), "memory", new Quantity(isBlank(resourceOptions.getSplunkMemoryLimit()) ? resourceOptions.getSplunkMemory() : resourceOptions.getSplunkMemoryLimit()));
            ContainerBuilder splunkBuilder = new ContainerBuilder().withName("splunk").withImage(splunkImage(options, resourceOptions)).withImagePullPolicy("IfNotPresent").withNewResources().addToRequests(splunkRequests).addToLimits(splunkLimits).endResources();
            splunkBuilder = splunkBuilder.withEnv(new EnvVarBuilder().withName("SPLUNK_CLUSTER").withValue(splunkCluster(options)).build(), new EnvVarBuilder().withName("SPLUNK_LOG_MONITOR").withValue(String.format("%s/stargate-*.log|%s|stargate-%s", STARGATE_LOGS, splunkIndex(options), type)).build());
            splunkBuilder = splunkBuilder.withVolumeMounts(getDefaultVolumeMounts());
            splunkBuilder = splunkBuilder.withLifecycle(new LifecycleBuilder().addToAdditionalProperties("type", "Sidecar").build());
            containerBuilders.add(splunkBuilder);
        }
        podBuilder = podBuilder.withVolumes(volumes);

        if (!isBlank(options.getServiceAccountName())) podBuilder = podBuilder.withServiceAccountName(options.getServiceAccountName().trim());
        if (options.getNodeSelector() != null && !options.getNodeSelector().isEmpty()) podBuilder = podBuilder.withNodeSelector(options.getNodeSelector());

        List<Container> initContainers = new ArrayList<>();

        if (WORKER.equals(type) && !isBlank(options.getJvmImage())) {
            Map<String, Quantity> initRequests = Map.of("cpu", new Quantity(resourceOptions.getInitCPU()), "memory", new Quantity(resourceOptions.getInitMemory()));
            Map<String, Quantity> initLimits = Map.of("cpu", new Quantity(isBlank(resourceOptions.getInitCPULimit()) ? resourceOptions.getInitCPU() : resourceOptions.getInitCPULimit()), "memory", new Quantity(isBlank(resourceOptions.getInitMemoryLimit()) ? resourceOptions.getInitMemory() : resourceOptions.getInitMemoryLimit()));
            ContainerBuilder initBuilder = new ContainerBuilder().withName("jvm-app-lib").withImage(options.getJvmImage()).withImagePullPolicy(imagePullPolicy).withNewResources().addToRequests(initRequests).addToLimits(initLimits).endResources();
            initBuilder = initBuilder.withCommand("/bin/sh").withArgs("-c", String.format("mkdir -p %s; mkdir -p %s%s; cp -R %s/* %s%s;", EXTERNAL_APP_LIB, STARGATE_RUNTIME_HOME, EXTERNAL_APP_LIB, EXTERNAL_APP_LIB, STARGATE_RUNTIME_HOME, EXTERNAL_APP_LIB));
            List<VolumeMount> initVMounts = getDefaultVolumeMounts();
            initBuilder = initBuilder.withVolumeMounts(initVMounts);
            initContainers.add(initBuilder.build());
        }
        if (initContainerRequired) {
            Map<String, Quantity> initRequests = Map.of("cpu", new Quantity(resourceOptions.getInitCPU()), "memory", new Quantity(resourceOptions.getInitMemory()));
            Map<String, Quantity> initLimits = Map.of("cpu", new Quantity(isBlank(resourceOptions.getInitCPULimit()) ? resourceOptions.getInitCPU() : resourceOptions.getInitCPULimit()), "memory", new Quantity(isBlank(resourceOptions.getInitMemoryLimit()) ? resourceOptions.getInitMemory() : resourceOptions.getInitMemoryLimit()));
            ContainerBuilder initBuilder = new ContainerBuilder().withName("copy-runtime").withImage(stargateImage).withImagePullPolicy(imagePullPolicy).withNewResources().addToRequests(initRequests).addToLimits(initLimits).endResources();
            initBuilder = initBuilder.withCommand("/bin/sh").withArgs("-c", String.format("/opt/setup-executor.sh %s", setupArguments.stream().collect(Collectors.joining(";"))));
            List<VolumeMount> initVMounts = getDefaultVolumeMounts();
            initVMounts.add(new VolumeMountBuilder().withName("stargate-base-shared-dir").withMountPath(DIRECTORY_PATH_SHARED_BASE).withReadOnly(false).build());
            initBuilder = initBuilder.withVolumeMounts(initVMounts);
            initContainers.add(initBuilder.build());
        }
        if (!initContainers.isEmpty()) podBuilder = podBuilder.withInitContainers(initContainers);

        podBuilder = podBuilder.withContainers(containerBuilders.stream().map(b -> b.build()).collect(Collectors.toList()));
        if (options.getNdots() >= 1) podBuilder = podBuilder.withNewDnsConfig().addNewOption("ndots", String.valueOf(options.getNdots())).endDnsConfig();
        if (COORDINATOR.equals(type)) {
            podBuilder = podBuilder.withRestartPolicy("OnFailure");
        }
        podBuilder = podBuilder.withShareProcessNamespace();
        PodSpec spec = podBuilder.build();
        return spec;
    }

    public static String stargateImage(final CoreOptions options) {
        if (!isBlank(options.getStargateImage())) return options.getStargateImage().trim();
        String image = config().getString("stargate.deployment.docker.image.executor").trim();
        return String.format("%s:%s", image, stargateVersion(options));
    }

    public static String mainImage(final CoreOptions options) {
        String image = options.getImage().trim();
        int versionIndex = image.lastIndexOf(':');
        if (versionIndex > 0) return image;
        return String.format("%s:%s", image, isBlank(options.getVersion()) ? "latest" : options.getVersion().trim());
    }

    public static String simplePortName(final String name) {
        String portName = name.toLowerCase().replaceAll("\\.", "-").replaceAll("-ports", "").replaceAll("-port", "");
        return "sg-" + Arrays.stream(portName.split("-")).map(x -> x.substring(0, Math.min(x.length(), 3))).collect(Collectors.joining("-"));
    }

    @SuppressWarnings("unchecked")
    public static Map<String, EnvVar> getEnvMap(final CoreOptions options, final String pipelineId, final long runNo, final PipelineConstants.RUNNER runner, final String sharedToken, final String type) {
        Map<String, EnvVar> env = new HashMap<>();
        PipelineConstants.DEPLOYMENT_TARGET deploymentTarget = options.deploymentTarget();

        runner.getConfig().env().forEach((k, v) -> env.put(k, new EnvVarBuilder().withName(k).withValue(v).build()));
        System.getenv().entrySet().stream().filter(e -> e.getKey().toUpperCase().startsWith("APP_ENV_")).forEach(e -> {
            String envName = e.getKey().substring(8);
            env.put(envName, new EnvVarBuilder().withName(envName).withValue(e.getValue()).build());
        });
        String stargateVersion = stargateVersion(options);
        Map<String, String> logMap = new HashMap<>();
        logMap.put(PIPELINE_ID, pipelineId);
        logMap.put(RUN_NO, String.valueOf(runNo));
        logMap.put("hostType", type);
        logMap.put(STARGATE_VERSION, stargateVersion);
        fillImageVersions(logMap, options);
        String logPrefix = "," + logMap.entrySet().stream().map(e -> String.format(" \"%s\":\"%s\"", e.getKey(), e.getValue())).collect(Collectors.joining(","));
        env.put(APP_ID, new EnvVarBuilder().withName(APP_ID).withValue(String.valueOf(appId(options))).build());
        env.put("APP_MODE", new EnvVarBuilder().withName("APP_MODE").withValue(mode(options)).build());
        env.put("STARGATE_VERSION", new EnvVarBuilder().withName("STARGATE_VERSION").withValue(stargateVersion).build());
        env.put(STARGATE_PIPELINE_ID, new EnvVarBuilder().withName(STARGATE_PIPELINE_ID).withValue(pipelineId).build());
        env.put("APP_LOG_NAME", new EnvVarBuilder().withName("APP_LOG_NAME").withValue(String.format("stargate-%s-executor", options.runner().name())).build());
        env.put("APP_LOG_ADDITIONAL_PREFIX", new EnvVarBuilder().withName("APP_LOG_ADDITIONAL_PREFIX").withValue(logPrefix).build());
        env.put("APP_LOG_OVERRIDES_FILE_PATH", new EnvVarBuilder().withName("APP_LOG_OVERRIDES_FILE_PATH").withValue(String.format("%s/%s/log-overrides.xml", DIRECTORY_PATH_SHARED_BASE, pipelineId)).build());
        env.put(STARGATE_SHARED_TOKEN, new EnvVarBuilder().withName(STARGATE_SHARED_TOKEN).withValue(sharedToken).build());
        env.put("JOB_MANAGER_RPC_ADDRESS", new EnvVarBuilder().withName("JOB_MANAGER_RPC_ADDRESS").withValue(coordinatorName(pipelineId)).build());
        if (deploymentTarget == stargate) env.put(STARGATE_OPERATOR_URI, new EnvVarBuilder().withName(STARGATE_OPERATOR_URI).withValue(env(STARGATE_OPERATOR_URI, "http://stargate-operator.stargate-system.svc.cluster.local")).build());

        if (!isBlank(options.getAwsRegion())) env.put("AWS_REGION", new EnvVarBuilder().withName("AWS_REGION").withValue(options.getAwsRegion()).build());
        if (!isBlank(options.getAwsDefaultRegion())) env.put("AWS_DEFAULT_REGION", new EnvVarBuilder().withName("AWS_DEFAULT_REGION").withValue(options.getAwsDefaultRegion()).build());
        if (!isBlank(options.getWebProxy())) {
            asList("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY").stream().forEach(key -> env.put(key, new EnvVarBuilder().withName(key).withValue(options.getWebProxy()).build()));
        }
        if (!isBlank(options.getSecretName()) && options.getSecretKeys() != null && !options.getSecretKeys().isEmpty()) {
            options.getSecretKeys().stream().forEach(key -> env.put(key, new EnvVarBuilder().withName(key).withValueFrom(new EnvVarSourceBuilder().withNewSecretKeyRef(options.getSecretName(), key, true).build()).build()));
        }
        return env;
    }

    public static String coordinatorName(final String pipelineId) {
        return String.format("%s-%s", pipelineId, COORDINATOR);
    }

    public static ContainerBuilder sidecarBuilder(final CoreOptions options, final CoreResourceOptions resourceOptions, final String executorType, final String image, final String imagePullPolicy, final String cpu, final String memory, final String cpuLimit, final String memoryLimit, final int portNo, final boolean enableDebugMode, final int debugPortNo, final Map<String, EnvVar> _envMap, final List<String> endpoints, final List<String> setupArguments) {
        setupArguments.add(executorType);

        Map<String, EnvVar> envMap = new HashMap<>(_envMap);
        String modifiedLogPrefix = String.format("%s, \"container\": \"%s\" ", envMap.get(APP_LOG_ADDITIONAL_PREFIX).getValue(), executorType);
        envMap.put(APP_LOG_ADDITIONAL_PREFIX, new EnvVarBuilder().withName(APP_LOG_ADDITIONAL_PREFIX).withValue(modifiedLogPrefix).build());
        List<EnvVar> env = new ArrayList<>(envMap.values());
        env.add(new EnvVarBuilder().withName(STARGATE_CONTAINER_PORT).withValue(String.valueOf(portNo)).build());
        env.add(new EnvVarBuilder().withName(STARGATE_MAIN_CONTAINER_GRPC_ENDPOINT).withValue(String.format("localhost:%d", GRPC)).build());
        if (!envMap.containsKey(STARGATE_CONTAINER_GRPC_THREAD_POOL_SIZE)) env.add(new EnvVarBuilder().withName(STARGATE_CONTAINER_GRPC_THREAD_POOL_SIZE).withValue(String.valueOf((options.getNumberOfSlots() * 5))).build());// factor is 5 is just an approximation; ideally that factor should be total no of python nodes
        endpoints.add(String.format("localhost:%d", portNo));

        Map<String, Quantity> requests = Map.of("cpu", new Quantity(isBlank(cpu) ? resourceOptions.getCpu() : cpu), "memory", new Quantity(isBlank(memory) ? resourceOptions.getMemory() : memory));
        Map<String, Quantity> limits = Map.of("cpu", new Quantity(isBlank(cpuLimit) ? requests.get("cpu").getAmount() : cpuLimit), "memory", new Quantity(isBlank(memoryLimit) ? requests.get("memory").getAmount() : memoryLimit));
        ContainerBuilder builder = new ContainerBuilder().withName(executorType).withImage(image).withImagePullPolicy(imagePullPolicy).withNewResources().addToRequests(requests).addToLimits(limits).endResources();
        List<String> args = new ArrayList<>();
        args.add("/opt/stargate-runtime/run-executor.sh");
        args.add(String.format("/opt/stargate-%s-executor/init.sh", executorType));
        builder = builder.withCommand("/bin/sh").withArgs("-c", args.stream().collect(Collectors.joining(" ")));
        List<ContainerPort> ports = new ArrayList<>();
        ports.add(new ContainerPortBuilder().withName(String.format("%s-comm", executorType)).withContainerPort(portNo).build());
        if (enableDebugMode) {
            env.add(new EnvVarBuilder().withName(STARGATE_CONTAINER_DEBUG_PORT).withValue(String.valueOf(debugPortNo)).build());
            ports.add(new ContainerPortBuilder().withName(String.format("%s-debug", executorType)).withContainerPort(debugPortNo).build());
        }
        builder = builder.withPorts(ports);
        builder = builder.withNewLivenessProbe().withNewTcpSocket().withNewPort(portNo).endTcpSocket().withInitialDelaySeconds(30).withPeriodSeconds(60).withTimeoutSeconds(5).endLivenessProbe();
        builder = builder.withEnv(env);
        builder = builder.withVolumeMounts(getDefaultVolumeMounts());
        builder = builder.withLifecycle(new LifecycleBuilder().addToAdditionalProperties("type", "Sidecar").build());
        return builder;
    }

    public static void addCoordinatorArgs(final String pipelineId, final long runNo, final CoreOptions options, final PipelineConstants.RUNNER runner, final List<String> args) {
        args.add(String.format("--pipelineId=%s", pipelineId));
        args.add(String.format("--pipelineRunNo=%d", runNo));
        args.add(String.format("--pipelineRunner=%s", runner.name()));
        args.add(String.format("--stargate.name=%s", pipelineId));
        Set<String> argNames = new HashSet<>();
        if (options.getArgs() != null && !options.getArgs().isEmpty()) {
            options.getArgs().stream().map(a -> String.format("--%s=\"%s\"", a.getName(), a.getValue())).forEach(x -> args.add(x));
            argNames.addAll(options.getArgs().stream().map(a -> a.getName()).collect(Collectors.toSet()));
        }
        if (options.getArguments() != null && !options.getArguments().isEmpty()) {
            options.getArguments().entrySet().stream().map(a -> String.format("--%s=\"%s\"", a.getKey(), a.getValue())).forEach(x -> args.add(x));
            argNames.addAll(options.getArguments().keySet());
        }
        if (options.getOverrides() != null && !options.getOverrides().isEmpty()) {
            options.getOverrides().entrySet().stream().map(a -> String.format("--stargate.%s=\"%s\"", a.getKey(), a.getValue())).forEach(x -> args.add(x));
            argNames.addAll(options.getOverrides().keySet().stream().map(a -> String.format("stargate.%s", a)).collect(Collectors.toSet()));
        }
        if (options.getLogLevels() != null && !options.getLogLevels().isEmpty()) {
            options.getLogLevels().entrySet().stream().map(a -> String.format("--logLevel.%s=\"%s\"", a.getKey(), a.getValue())).forEach(x -> args.add(x));
            argNames.addAll(options.getLogLevels().keySet().stream().map(a -> String.format("logLevel.%s", a)).collect(Collectors.toSet()));
        }
        if (!argNames.contains("fasterCopy")) args.add("--fasterCopy=true");
        if (!argNames.contains("objectReuse")) args.add("--objectReuse=true");
        if (!argNames.contains("operatorChaining")) args.add("--operatorChaining=true");
        if (!argNames.contains("autoBalanceWriteFilesShardingEnabled")) args.add("--autoBalanceWriteFilesShardingEnabled=true");
        if (!argNames.contains("checkpointDir")) args.add("--checkpointDir=" + checkpointDirectory(pipelineId));
        if (!argNames.contains("systemPropertiesFile") && options.getWhisperOptions() != null && !options.getWhisperOptions().isEmpty()) {
            String systemPropertiesFile = options.getWhisperOptions().getOrDefault("filePath", config().getString("stargate.deployment.entrypoint.whisper.default.filePath"));
            if (!isBlank(systemPropertiesFile)) args.add(String.format("--systemPropertiesFile=%s", systemPropertiesFile));
        }
    }

    public static String configMapName(final String pipelineId) {
        return String.format("stargate-%s", pipelineId);
    }

    public static String pvcClaim(final CoreOptions options) {
        if (!isBlank(options.getPvcClaim())) {
            return options.getPvcClaim().trim();
        }
        return env("APP_PVC_CLAIM", null);
    }

    public static List<VolumeMount> getDefaultVolumeMounts() {
        List<VolumeMount> mounts = new ArrayList<>();
        mounts.add(new VolumeMountBuilder().withName("stargate-runtime").withMountPath(STARGATE_RUNTIME_HOME).withReadOnly(false).build());
        mounts.add(new VolumeMountBuilder().withName("splunk-logs").withMountPath(STARGATE_LOGS).withReadOnly(false).build());
        mounts.add(new VolumeMountBuilder().withName("splunk-temp").withMountPath("/opt/splunk/var").withReadOnly(false).build());
        mounts.add(new VolumeMountBuilder().withName("pod-info").withMountPath("/etc/splunk/podinfo").withReadOnly(true).build());
        return mounts;
    }

    @SuppressWarnings("unchecked")
    public static Map<String, String> getConfigDataMap(final String pipelineId, final CoreOptions options) throws Exception {
        PipelineConstants.RUNNER runner = options.runner();
        RunnerConfig runnerConfig = runner.getConfig();
        Map<String, Map<String, String>> configData = runnerConfig.configData() == null ? new HashMap<>() : duplicate(runnerConfig.configData(), Map.class);
        Set<String> ignoreKeys = new HashSet<>();
        if (runner == PipelineConstants.RUNNER.flink || runner == PipelineConstants.RUNNER.nativeflink) {
            Map<String, String> config = configData.get(FLINK_CONF_FILE);
            runnerConfig.getCoordinator().ports().forEach((k, v) -> config.put(k, String.valueOf(v)));
            runnerConfig.getWorker().ports().forEach((k, v) -> config.put(k, String.valueOf(v)));
            config.put("jobmanager.rpc.address", coordinatorName(pipelineId));
            config.put("state.checkpoints.dir", checkpointDirectory(pipelineId));
            options.calculateParallelism();
            config.put("parallelism.default", String.valueOf(options.getParallelism()));
            config.put("taskmanager.numberOfTaskSlots", String.valueOf(options.getNumberOfSlots()));
            config.put("jobmanager.memory.process.size", translatedMemory(options.coordinator().getMemoryLimit()));
            config.put("taskmanager.memory.process.size", translatedMemory(options.worker().getMemoryLimit()));
            if (parseBoolean(options.getRunnerConfigs().get("flinkReactiveMode"))) {
                if (!config.containsKey("scheduler-mode")) config.put("scheduler-mode", "reactive");
                if (!config.containsKey("jobmanager.scheduler")) config.put("jobmanager.scheduler", "adaptive");
                if (!config.containsKey("cluster.declarative-resource-management.enabled")) config.put("cluster.declarative-resource-management.enabled", "true");
                ignoreKeys.add("flinkReactiveMode");
            }
        }
        Map<String, String> tempMap = new HashMap<>();
        options.getRunnerConfigs().entrySet().stream().filter(e -> !ignoreKeys.contains(e.getKey())).forEach(e -> tempMap.put(e.getKey(), String.valueOf(e.getValue())));
        configData.forEach((k, v) -> v.putAll(tempMap));
        Map<String, String> configMap = new HashMap<>();
        for (Map.Entry<String, Map<String, String>> entry : configData.entrySet()) {
            configMap.put(entry.getKey(), simpleYamlString(entry.getValue()));
        }
        return configMap;
    }

    public static String configMapKey(final String path) {
        return Arrays.stream(path.split("/")).filter(t -> !t.trim().isBlank()).collect(Collectors.joining("__"));
    }

    public static String splunkImage(final CoreOptions options, final CoreResourceOptions resourceOptions) {
        return String.format(config().getString("stargate.deployment.docker.image.splunkForwarder"), "heavy".equalsIgnoreCase(resourceOptions.getLogForwarder()) ? "heavy" : "universal") + ":" + splunkVersion(options);
    }

    public static String splunkCluster(final CoreOptions options) {
        if (!isBlank(options.getSplunkCluster())) {
            return options.getSplunkCluster().trim();
        }
        return environment().getConfig().getSplunkCluster();
    }

    public static String splunkIndex(final CoreOptions options) {
        if (!isBlank(options.getSplunkIndex())) {
            return options.getSplunkIndex().trim();
        }
        return environment().getConfig().getStargateSplunkIndex();
    }

    public static String stargateVersion(final CoreOptions options) {
        if (!isBlank(options.getStargateVersion())) return options.getStargateVersion().trim();
        Set<String> versions = new HashSet<>();
        versions.add(VERSION_INFO.getVersion());
        String envVersion = env(STARGATE_IMAGE_VERSION, null);
        if (!isBlank(envVersion)) versions.add(envVersion);
        if (versions.size() == 1) return versions.iterator().next();
        return versions.stream().map(s -> {
            try {
                return Pair.of(ModuleDescriptor.Version.parse(s), s);
            } catch (Exception e) {
                return Pair.of(ModuleDescriptor.Version.parse(String.format("%d.0.%s", Integer.MAX_VALUE, s)), s);
            }
        }).sorted(Comparator.reverseOrder()).findFirst().get().getRight().toString();
    }

    public static long appId(final CoreOptions options) {
        if (options.getAppId() > 0) {
            return options.getAppId();
        }
        return AppConfig.appId();
    }

    public static String mode(final CoreOptions options) {
        if (!isBlank(options.getMode())) {
            return options.getMode().trim().toUpperCase();
        }
        return AppConfig.mode();
    }

    public static String checkpointDirectory(final String pipelineId) {
        return String.format("file://%s/%s/checkpoints/", DIRECTORY_PATH_SHARED_BASE, pipelineId);
    }

    public static String translatedMemory(final String input) {
        Matcher matcher = UNIT_PATTERN.matcher(input);
        return matcher.matches() ? (matcher.group(1) + SIZE_CONVERSION_BASE2_TO_H.getOrDefault(matcher.group(2), "Gi")) : input;
    }

    public static String splunkVersion(final CoreOptions options) {
        if (!isBlank(options.getSplunkVersion())) {
            return options.getSplunkVersion().trim();
        }
        return env("APP_K8S_SPLUNK_FORWARDER_VERSION", env("APP_SPLUNK_VERSION", "latest"));
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public static Map<String, String> defaultAnnotations(final String type, final CoreOptions options) {
        Map<String, String> annotations = options.getAnnotations() == null ? new HashMap<>() : new HashMap<>(options.getAnnotations());
        if (type != null) {
            CoreResourceOptions resourceOptions = COORDINATOR.equals(type) ? options.coordinator() : options.worker();
            if (resourceOptions.getAnnotations() != null) getAs(resourceOptions.getAnnotations(), Map.class).forEach((k, v) -> annotations.put(String.valueOf(k), String.valueOf(v)));
        }
        annotations.putAll(PROMETHEUS_ANNOTATIONS);
        return annotations;
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public static Map<String, String> fillDefaultLabels(final String type, final CoreOptions options, final Map<String, String> labels) {
        if (options.getLabels() != null) labels.putAll(options.getLabels());
        if (type != null) {
            CoreResourceOptions resourceOptions = COORDINATOR.equals(type) ? options.coordinator() : options.worker();
            if (resourceOptions.getLabels() != null) getAs(resourceOptions.getLabels(), Map.class).forEach((k, v) -> labels.put(String.valueOf(k), String.valueOf(v)));
        }
        labels.put(PLATFORM, STARGATE);
        labels.put(K8sUtils.STARGATE_MANAGED, "true");
        fillImageVersions(labels, options);
        return labels;
    }

    private static Map<String, String> fillImageVersions(final Map<String, String> map, final CoreOptions options) {
        if (!isBlank(options.getImage()) && options.getImage().contains(":")) map.put("imageVersion", options.getImage().split(":")[1]);
        if (!isBlank(options.getJvmImage()) && options.getJvmImage().contains(":")) map.put("jvmImageVersion", options.getJvmImage().split(":")[1]);
        if (!isBlank(options.getPythonImage()) && options.getPythonImage().contains(":")) map.put("pythonImageVersion", options.getPythonImage().split(":")[1]);
        if (!isBlank(options.getExternalJvmImage()) && options.getExternalJvmImage().contains(":")) map.put("externalJvmImageVersion", options.getExternalJvmImage().split(":")[1]);
        if (!isBlank(options.getExternalImage()) && options.getExternalImage().contains(":")) map.put("externalImageVersion", options.getExternalImage().split(":")[1]);
        return map;
    }

}
