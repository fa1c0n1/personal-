package com.apple.aml.stargate.common.utils;

import com.apple.aiml.spi.surface.client.rest.ApiClient;
import com.apple.aiml.spi.surface.client.rest.api.DeploymentApi;
import com.apple.aiml.spi.surface.client.rest.model.DeploymentRequest;
import com.apple.aiml.spi.surface.client.rest.model.JobEntrypoint;
import com.apple.aiml.spi.surface.client.rest.model.JobManager;
import com.apple.aiml.spi.surface.client.rest.model.ServicePort;
import com.apple.aiml.spi.surface.client.rest.model.TaskManager;
import com.apple.aiml.spi.surface.client.rest.model.WhisperSecretsConfig;
import com.apple.ais.accountsecurity.REST;
import com.apple.aml.stargate.common.constants.CommonConstants;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.pojo.CoreOptions;
import com.apple.aml.stargate.common.pojo.CoreResourceOptions;
import com.typesafe.config.Config;
import okhttp3.OkHttpClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.apple.aml.stargate.common.constants.CommonConstants.IDMS_APP_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sLabelValues.COORDINATOR;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sLabelValues.WORKER;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sLabels.MODE;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sLabels.PLATFORM;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sLabels.RUN_NO;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sLabels.STARGATE_VERSION;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sLabels.VERSION_NO;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sLabels.WORKFLOW;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sPorts.RM;
import static com.apple.aml.stargate.common.constants.PipelineConstants.DirectoryPaths.FLINK_CONF_FILE;
import static com.apple.aml.stargate.common.constants.PipelineConstants.SURFACE_COMPONENT;
import static com.apple.aml.stargate.common.constants.PipelineConstants.SURFACE_COMPONENT_JOBMANAGER;
import static com.apple.aml.stargate.common.constants.PipelineConstants.SURFACE_DEPLOYMENT_APP;
import static com.apple.aml.stargate.common.constants.PipelineConstants.SURFACE_DEPLOYMENT_NAME;
import static com.apple.aml.stargate.common.utils.AppConfig.config;
import static com.apple.aml.stargate.common.utils.AppConfig.mode;
import static com.apple.aml.stargate.common.utils.ClassUtils.getAs;
import static com.apple.aml.stargate.common.utils.JsonUtils.yamlToMap;
import static com.apple.aml.stargate.common.utils.K8sUtils.STARGATE_MANAGED;
import static com.apple.aml.stargate.common.utils.K8sUtils.addCoordinatorArgs;
import static com.apple.aml.stargate.common.utils.K8sUtils.defaultAnnotations;
import static com.apple.aml.stargate.common.utils.K8sUtils.fillDefaultLabels;
import static com.apple.aml.stargate.common.utils.K8sUtils.getConfigDataMap;
import static com.apple.aml.stargate.common.utils.K8sUtils.getPodTemplate;
import static com.apple.aml.stargate.common.utils.K8sUtils.stargateImage;
import static java.util.Arrays.asList;

public final class SurfaceUtils {

    public static String getSurfDawToken() {
        try {
            Config sysAccountConfig = AppConfig.config().getConfig("stargate.sysaccount");
            String env = sysAccountConfig.getString("env");
            String name = sysAccountConfig.getString("name");
            String secret = sysAccountConfig.getString("secret");
            String password = sysAccountConfig.getString("password");
            String appIdKey = sysAccountConfig.getString("appIdKey");
            REST re = new REST.Builder().environment(env).sharedsecret(secret).systemusername(name).systempassword(password).appidkey(appIdKey).build();
            return re.getDAW();
        } catch (Exception e) {
            throw new SecurityException("Unable to generate Daw Token! Check if config is missing!!");
        }
    }

    @SuppressWarnings("unchecked")
    public static DeploymentRequest createDeploymentRequest(final CoreOptions options) throws Exception {
        String pipelineId = options.getPipelineId();
        options.setRunner(PipelineConstants.RUNNER.flink.name());
        options.setDeploymentTarget(PipelineConstants.DEPLOYMENT_TARGET.surface.name());
        String imageDetails[] = stargateImage(options).split(":");
        //TODO Implement logic to generate runNo and versionNo dynamically.
        int runNo = 0;
        int versionNo = 0;
        Map<String, String> coordinatorLabels = new HashMap<>();
        Map<String, String> workerLabels = new HashMap<>();
        fillDefaultLabels(COORDINATOR, options, coordinatorLabels);
        fillDefaultLabels(WORKER, options, workerLabels);
        Map<String, String> coordinatorAnnotations = defaultAnnotations(COORDINATOR, options);
        Map<String, String> workerAnnotations = defaultAnnotations(WORKER, options);
        for (Map<String, String> labels : asList(coordinatorLabels, workerLabels)) {
            labels.put(PLATFORM, CommonConstants.STARGATE);
            labels.put(STARGATE_MANAGED, "true");
            labels.put(PipelineConstants.PIPELINE_ID, pipelineId);
            labels.put(IDMS_APP_ID, String.valueOf(options.getAppId()));
            labels.put(MODE, mode().toUpperCase());
            labels.put(WORKFLOW, String.format("%s-%s", CommonConstants.STARGATE, pipelineId));
            labels.put(CommonConstants.K8sLabels.RUNNER, PipelineConstants.RUNNER.flink.name());
            labels.put(RUN_NO, String.valueOf(runNo));
            labels.put(VERSION_NO, String.valueOf(versionNo));
            labels.put(STARGATE_VERSION, imageDetails[1]);
        }
        DeploymentRequest spec = new DeploymentRequest();
        spec.setDockerImageUri(imageDetails[0]);
        spec.setDockerImageTag(imageDetails[1]);
        spec.setImagePullPolicy(options.imagePullPolicy());

        //Add whisper annotations
        if (options.getWhisperOptions() != null && !options.getWhisperOptions().isEmpty()) {
            WhisperSecretsConfig whisperSecretsConfig = getAs(options.getWhisperOptions(), WhisperSecretsConfig.class);
            spec.setWhisperSecretsConfig(whisperSecretsConfig);
        }

        //Create and set entrypoint details to spec
        JobEntrypoint entrypoint = new JobEntrypoint();
        entrypoint.setJarUri(config().getString("stargate.deployment.entrypoint.jarUri"));
        entrypoint.setEntryClass(config().getString("stargate.deployment.entrypoint.className"));
        List<String> arguments = new ArrayList<>();
        addCoordinatorArgs(pipelineId, runNo, options, PipelineConstants.RUNNER.flink, arguments);
        entrypoint.setMainArgs(arguments);
        spec.setJobEntrypoint(entrypoint);

        String sharedToken = UUID.randomUUID().toString();

        CoreResourceOptions coordinator = options.coordinator();

        //Create and set job manager details to spec
        JobManager jmSpec = new JobManager();
        jmSpec.setMemory(coordinator.getMemoryLimit().substring(0, 2));
        jmSpec.setCpu(coordinator.getCpu().toLowerCase().endsWith("m") ? (Double.parseDouble(coordinator.getCpu().replaceAll("m", "")) / 1000.0) : Double.parseDouble(coordinator.getCpu()));
        jmSpec.setPodTemplateSpec(getAs(getPodTemplate(COORDINATOR, 0, options, sharedToken, coordinatorLabels, coordinatorAnnotations), Map.class));
        spec.setJobManager(jmSpec);

        //Create and set task manager details to spec
        CoreResourceOptions worker = options.worker();
        TaskManager tmSpec = new TaskManager();
        tmSpec.setMemory(worker.getMemoryLimit().substring(0, 2));
        tmSpec.setCpu(worker.getCpu().toLowerCase().endsWith("m") ? (Double.parseDouble(worker.getCpu().replaceAll("m", "")) / 1000.0) : Double.parseDouble(worker.getCpu()));
        tmSpec.setPodTemplateSpec(getAs(getPodTemplate(WORKER, 0, options, sharedToken, workerLabels, workerAnnotations), Map.class));
        spec.setTaskManager(tmSpec);

        //Set flink configuration details to spec
        spec.setFlinkConfiguration(yamlToMap(getConfigDataMap(pipelineId, options).get(FLINK_CONF_FILE)));

        //Set service ports
        ServicePort jmServicePortSpec = new ServicePort();
        jmServicePortSpec.port(RM);
        jmServicePortSpec.portName(COORDINATOR);
        jmServicePortSpec.putSelectorItem(SURFACE_COMPONENT, SURFACE_COMPONENT_JOBMANAGER);
        jmServicePortSpec.putSelectorItem(SURFACE_DEPLOYMENT_NAME, pipelineId);
        jmServicePortSpec.putSelectorItem(SURFACE_DEPLOYMENT_APP, pipelineId);

        spec.setServicePorts(List.of(jmServicePortSpec));

        return spec;
    }

    public static DeploymentApi getDeploymentApi(OkHttpClient surfaceHttpClient, String surfaceBasePath) {
        // Wrap the http client in the object that the library expects
        ApiClient apiClient = new ApiClient(surfaceHttpClient).setBasePath(surfaceBasePath);

        // Create a new deployment API
        // Other apis are located in the same Java package
        return new DeploymentApi(apiClient);

    }

}
