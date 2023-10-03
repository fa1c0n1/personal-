package com.apple.aml.stargate.k8s.reconciler;

import com.apple.aml.stargate.common.constants.A3Constants;
import com.apple.aml.stargate.common.exceptions.InvalidInputException;
import com.apple.aml.stargate.common.pojo.CoreOptions;
import com.apple.aml.stargate.common.utils.AppConfig;
import com.apple.aml.stargate.common.utils.WebUtils;
import com.apple.aml.stargate.k8s.conditions.ConfigMapPrecondition;
import com.apple.aml.stargate.k8s.conditions.CoordinatorJobPrecondition;
import com.apple.aml.stargate.k8s.conditions.CoordinatorServicePrecondition;
import com.apple.aml.stargate.k8s.conditions.NetworkPolicyPrecondition;
import com.apple.aml.stargate.k8s.conditions.WorkerDeploymentPrecondition;
import com.apple.aml.stargate.k8s.crd.CoreResource;
import com.apple.aml.stargate.k8s.crd.CoreStatus;
import com.apple.aml.stargate.k8s.dependents.ConfigMapResource;
import com.apple.aml.stargate.k8s.dependents.CoordinatorJobResource;
import com.apple.aml.stargate.k8s.dependents.CoordinatorServiceResource;
import com.apple.aml.stargate.k8s.dependents.NetworkPolicyResource;
import com.apple.aml.stargate.k8s.dependents.WorkerDeploymentResource;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Cleaner;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusHandler;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.MaxReconciliationInterval;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.dependent.Dependent;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.apple.aml.stargate.common.constants.CommonConstants.K8sLabels.RUN_NO;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sLabels.VERSION_NO;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sOperator.API_VERSION;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sOperator.KIND;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.PipelineConstants.PIPELINE_ID;
import static com.apple.aml.stargate.common.utils.A3Utils.a3Mode;
import static com.apple.aml.stargate.common.utils.A3Utils.getA3Token;
import static com.apple.aml.stargate.common.utils.AppConfig.environment;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.nonNullMap;
import static com.apple.aml.stargate.common.utils.K8sUtils.STATUS.COMPLETED;
import static com.apple.aml.stargate.common.utils.K8sUtils.STATUS.ERROR;
import static com.apple.aml.stargate.common.utils.K8sUtils.STATUS.INITIALIZING;
import static com.apple.aml.stargate.common.utils.K8sUtils.STATUS.RUNNING;
import static com.apple.aml.stargate.common.utils.K8sUtils.STATUS.WAITING;
import static com.apple.aml.stargate.common.utils.K8sUtils.appId;
import static com.apple.aml.stargate.common.utils.K8sUtils.configMapName;
import static com.apple.aml.stargate.common.utils.K8sUtils.coordinatorName;
import static com.apple.aml.stargate.common.utils.K8sUtils.mode;
import static com.apple.aml.stargate.common.utils.K8sUtils.stargateVersion;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.TemplateUtils.yyyyMMddHHmmss;
import static com.apple.aml.stargate.k8s.utils.ReconcilerUtils.defaultLabels;
import static com.apple.aml.stargate.k8s.utils.ReconcilerUtils.getOperatorOptions;
import static com.apple.aml.stargate.k8s.utils.ReconcilerUtils.networkPolicyName;
import static com.apple.aml.stargate.k8s.utils.ReconcilerUtils.workerName;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static java.lang.Long.parseLong;
import static java.nio.charset.StandardCharsets.UTF_8;

@ControllerConfiguration(maxReconciliationInterval = @MaxReconciliationInterval(interval = 5L, timeUnit = TimeUnit.MINUTES), dependents = {@Dependent(type = ConfigMapResource.class, reconcilePrecondition = ConfigMapPrecondition.class), @Dependent(type = CoordinatorJobResource.class, reconcilePrecondition = CoordinatorJobPrecondition.class), @Dependent(type = CoordinatorServiceResource.class, reconcilePrecondition = CoordinatorServicePrecondition.class), @Dependent(type = NetworkPolicyResource.class, reconcilePrecondition = NetworkPolicyPrecondition.class), @Dependent(type = WorkerDeploymentResource.class, reconcilePrecondition = WorkerDeploymentPrecondition.class)})
@org.springframework.stereotype.Service
public class CoreReconciler implements Reconciler<CoreResource>, Cleaner<CoreResource>, ErrorStatusHandler<CoreResource> {
    private final Logger logger = logger(MethodHandles.lookup().lookupClass());
    private final Map<String, String> sharedTokenCache = new ConcurrentHashMap<>();
    @Autowired
    private KubernetesClient client;

    @SuppressWarnings("unchecked")
    @Override
    public UpdateControl<CoreResource> reconcile(final CoreResource resource, final Context<CoreResource> context) throws Exception {
        Instant now = Instant.now();
        String pipelineId = resource.pipelineId();
        String namespace = resource.getMetadata().getNamespace();
        Map<String, Object> log = new HashMap<>();
        log.put(PIPELINE_ID, pipelineId);
        log.put("namespace", namespace);
        CoreStatus status = resource.getStatus();
        if (COMPLETED.name().equalsIgnoreCase(status.getStatus()) && resource.sameRequestId()) {
            logger.debug("Pipeline is already marked completed. Nothing to reconcile !", log);
            return UpdateControl.noUpdate();
        }
        logger.debug("Starting reconciliation for ", log);
        ConfigMap configMap = null;
        Job coordinator = null;
        Deployment worker = null;
        NetworkPolicy networkPolicy = null;
        Service service = null;
        CoreOptions options = getOperatorOptions(resource);
        boolean updateResource = false;
        if (resource.getMetadata().getLabels() == null) {
            updateResource = true;
            resource.getMetadata().setLabels(new HashMap<>());
        }
        Map optionsMap = nonNullMap(resource.getSpec());
        optionsMap.remove("definition");
        optionsMap.remove("spec");
        String encodedOptions = Base64.getEncoder().encodeToString(jsonString(new TreeMap<>(optionsMap)).getBytes(UTF_8));

        if (isBlank(status.getStatus()) || status.getRunNo() <= 0) {
            logger.info("Applying new stargate deployment", log);
            String functionalDAG = options.functionalDAG();
            if (isBlank(functionalDAG)) {
                status.setEncodedDAG(null);
            } else {
                logger.info("Pipeline definition override supplied. Will try to register the new pipeline definition..", log);
                registerPipelineSpec(pipelineId, functionalDAG);
                status.setEncodedDAG(Base64.getEncoder().encodeToString(functionalDAG.getBytes(UTF_8)));
            }
            status.setEncodedOptions(encodedOptions);
            Pair<Long, Long> runInfo = getRunInfo(pipelineId, options);
            status.setRequestId(options.getRequestId());
            status.setAppId(appId(options));
            status.setMode(mode(options));
            status.setPipelineId(pipelineId);
            status.setSharedToken(sharedTokenCache.computeIfAbsent(pipelineId, k -> UUID.randomUUID().toString()));
            status.setVersionNo(runInfo.getLeft());
            status.setRunNo(runInfo.getRight());
            status.setRunner(options.runner().name());
            status.setStargateVersion(stargateVersion(options));
            status.setStartedOn(yyyyMMddHHmmss(now.toEpochMilli()));
            resource.getMetadata().getLabels().putAll(defaultLabels(null, options, resource));
            updateResource = true;
        } else {
            boolean restart = false;
            String functionalDAG = options.functionalDAG();
            if (!isBlank(functionalDAG)) {
                String encodedDAG = Base64.getEncoder().encodeToString(functionalDAG.getBytes(UTF_8));
                if (!encodedDAG.equals(status.getEncodedDAG())) {
                    logger.info("Pipeline definition override supplied & is different from current running functional DAG. Will try to register the updated pipeline definition and schedule for a restart", log, Map.of("currentStatus", String.valueOf(status)));
                    registerPipelineSpec(pipelineId, functionalDAG);
                    status.setEncodedDAG(encodedDAG);
                    restart = true;
                }
            }
            if (isBlank(options.getStargateVersion())) {
                String stargateVersion = stargateVersion(options);
                if (!COMPLETED.name().equalsIgnoreCase(status.getStatus()) && !stargateVersion.equalsIgnoreCase(status.getStargateVersion())) {
                    restart = true;
                    logger.info("Stargate version of current running pipeline is different than what is resolved by operator. Will  schedule for a restart", log, Map.of("currentStatus", String.valueOf(status), "resolvedVersion", stargateVersion, "currentVersion", String.valueOf(status.getStargateVersion())));
                    status.setStargateVersion(stargateVersion);
                }
            }

            if (!encodedOptions.equals(status.getEncodedOptions())) {
                restart = true;
                logger.info("Pipeline k8s optionals are different from current running k8s resources. Will  schedule for a restart", log, Map.of("currentStatus", String.valueOf(status), "resolvedOptions", encodedOptions, "currentOptions", String.valueOf(status.getEncodedOptions())));
                status.setEncodedOptions(encodedOptions);
            }

            service = client.services().inNamespace(namespace).withName(coordinatorName(pipelineId)).get();
            networkPolicy = client.network().v1().networkPolicies().inNamespace(namespace).withName(networkPolicyName(pipelineId)).get();

            if (restart) {
                logger.info("Pipeline is marked for restart. Will delete existing k8s resources", log);
                client.apps().deployments().inNamespace(namespace).withName(workerName(pipelineId)).delete();
                worker = null;
                client.batch().v1().jobs().inNamespace(namespace).withName(coordinatorName(pipelineId)).delete();
                client.apps().statefulSets().inNamespace(namespace).withName(coordinatorName(pipelineId)).delete(); // TODO : Need to delete in next major version upgrade; added for backward compatibility
                coordinator = null;
                client.configMaps().inNamespace(namespace).withName(configMapName(pipelineId)).delete();
                configMap = null;
                Pair<Long, Long> runInfo = getRunInfo(pipelineId, options);
                status.setVersionNo(runInfo.getLeft());
                status.setRunNo(runInfo.getRight());
                resource.getMetadata().getLabels().putAll(defaultLabels(null, options, resource));
                updateResource = true;
            } else {
                worker = client.apps().deployments().inNamespace(namespace).withName(workerName(pipelineId)).get();
                coordinator = client.batch().v1().jobs().inNamespace(namespace).withName(coordinatorName(pipelineId)).get();
                configMap = client.configMaps().inNamespace(namespace).withName(configMapName(pipelineId)).get();
            }
        }

        boolean reschedule = false;
        String statusString = null;
        if (coordinator != null && service != null && resource.sameRequestId()) {
            statusString = WebUtils.httpGet(String.format("http://%s.%s.svc.cluster.local:8888/status", coordinatorName(pipelineId), namespace), null, String.class, false, false);
            if (isBlank(statusString)) statusString = (coordinator.getStatus() != null && coordinator.getStatus().getSucceeded() != null && coordinator.getStatus().getSucceeded() >= 1) ? COMPLETED.name() : null;
        }
        if (COMPLETED.name().equalsIgnoreCase(statusString)) {
            logger.info("Pipeline status is marked as COMPLETED! Will cleanup all child resources", log);
            client.apps().statefulSets().inNamespace(namespace).withName(coordinatorName(pipelineId)).delete(); // TODO : Need to delete in next major version upgrade; added for backward compatibility
            if (worker != null) client.apps().deployments().inNamespace(namespace).withName(workerName(pipelineId)).delete();
            if (coordinator != null) client.batch().v1().jobs().inNamespace(namespace).withName(coordinatorName(pipelineId)).delete();
            if (configMap != null) client.configMaps().inNamespace(namespace).withName(configMapName(pipelineId)).delete();
            if (networkPolicy != null) client.network().v1().networkPolicies().inNamespace(namespace).withName(networkPolicyName(pipelineId)).delete();
            if (service != null) client.services().inNamespace(namespace).withName(coordinatorName(pipelineId)).delete();
        } else {
            if (configMap == null || worker == null || coordinator == null || service == null || networkPolicy == null) {
                statusString = INITIALIZING.name();
                reschedule = true;
            } else if (context.managedDependentResourceContext().getWorkflowReconcileResult().orElseThrow().allDependentResourcesReady()) {
                statusString = RUNNING.name();
            } else {
                statusString = WAITING.name();
                reschedule = true;
            }
        }
        status.setRequestId(options.getRequestId());
        status.setStatus(statusString);
        status.setUpdatedOn(yyyyMMddHHmmss(now.toEpochMilli()));
        logger.info("Reconciliation completed for ", log, Map.of("currentStatus", String.valueOf(status)));
        UpdateControl<CoreResource> control = updateResource ? UpdateControl.updateResourceAndStatus(resource) : UpdateControl.updateStatus(resource);
        if (reschedule) control = control.rescheduleAfter(1, TimeUnit.SECONDS);
        return control;
    }

    @SuppressWarnings("unchecked")
    private static Pair<Long, Long> getRunInfo(final String pipelineId, final CoreOptions options) throws Exception {
        Map<String, Object> runInfo = new HashMap<>();
        if (options.getOverrides() != null) runInfo.putAll(options.getOverrides());
        long versionNo = runInfo.containsKey(VERSION_NO) ? parseLong(String.valueOf(runInfo.get(VERSION_NO))) : -1L;
        long runNo = runInfo.containsKey(RUN_NO) ? parseLong(String.valueOf(runInfo.get(RUN_NO))) : -1L;
        if (versionNo <= 0 || runNo <= 0) {
            runInfo.putAll(WebUtils.postJsonData(String.format("%s/api/v1/pipeline/deployment/run-info/%s", environment().getConfig().getStargateUri(), pipelineId), Map.of(), AppConfig.appId(), getA3Token(A3Constants.KNOWN_APP.STARGATE.appId()), a3Mode(), Map.class));
            versionNo = parseLong(String.valueOf(runInfo.getOrDefault(VERSION_NO, -1L)));
            runNo = parseLong(String.valueOf(runInfo.getOrDefault(RUN_NO, -1L)));
        }
        return Pair.of(versionNo, runNo);
    }

    @SuppressWarnings("unchecked")
    private static void registerPipelineSpec(final String pipelineId, final String pipelineSpec) throws Exception {
        Map<String, Object> response = WebUtils.postJsonData(String.format("%s/api/v1/pipeline/deployment/register/%s", environment().getConfig().getStargateUri(), pipelineId), pipelineSpec, AppConfig.appId(), getA3Token(A3Constants.KNOWN_APP.STARGATE.appId()), a3Mode(), Map.class);
        if (response == null || response.isEmpty()) {
            throw new InvalidInputException("Could not register the pipeline definition override!!");
        }
    }

    @Override
    public DeleteControl cleanup(final CoreResource resource, final Context<CoreResource> context) {
        String pipelineId = resource.pipelineId();
        String namespace = resource.getMetadata().getNamespace();
        CoreStatus status = resource.getStatus();
        logger.info("Cleaning up deployment for ", Map.of(PIPELINE_ID, pipelineId, "namespace", namespace, "currentStatus", String.valueOf(status)));
        sharedTokenCache.remove(pipelineId);
        return DeleteControl.defaultDelete();
    }

    public String sharedToken(final String pipelineId) {
        return sharedTokenCache.computeIfAbsent(pipelineId, id -> {
            Optional<GenericKubernetesResource> resource = client.genericKubernetesResources(API_VERSION, KIND).inAnyNamespace().list().getItems().stream().filter(r -> r.getMetadata().getName().equalsIgnoreCase(pipelineId)).findFirst();
            if (resource == null || resource.isEmpty()) throw new IllegalArgumentException(String.format("There is no pipeline deployed with name : %s", pipelineId));
            return resource.get().get("status", "sharedToken");
        });
    }

    @Override
    public ErrorStatusUpdateControl<CoreResource> updateErrorStatus(final CoreResource resource, final Context<CoreResource> context, final Exception exception) {
        String pipelineId = resource.pipelineId();
        String namespace = resource.getMetadata().getNamespace();
        CoreStatus status = resource.getStatus();
        logger.error("Error encountered in reconciling deployment for ", Map.of(PIPELINE_ID, pipelineId, "namespace", namespace, "currentStatus", String.valueOf(status)), ERROR_MESSAGE, exception.getMessage(), exception);
        status.setStatus(ERROR.name());
        return ErrorStatusUpdateControl.patchStatus(resource);
    }
}
