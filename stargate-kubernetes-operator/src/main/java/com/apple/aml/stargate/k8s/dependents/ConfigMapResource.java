package com.apple.aml.stargate.k8s.dependents;

import com.apple.aml.stargate.common.pojo.CoreOptions;
import com.apple.aml.stargate.k8s.crd.CoreResource;
import com.apple.aml.stargate.k8s.crd.CoreStatus;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.PipelineConstants.PIPELINE_ID;
import static com.apple.aml.stargate.common.utils.JsonUtils.yamlString;
import static com.apple.aml.stargate.common.utils.K8sUtils.STARGATE_MANAGED;
import static com.apple.aml.stargate.common.utils.K8sUtils.getConfigDataMap;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.k8s.utils.ReconcilerUtils.defaultLabels;
import static com.apple.aml.stargate.k8s.utils.ReconcilerUtils.getConfigMap;
import static com.apple.aml.stargate.k8s.utils.ReconcilerUtils.getOperatorOptions;

@KubernetesDependent(labelSelector = STARGATE_MANAGED)
public class ConfigMapResource extends CRUDKubernetesDependentResource<ConfigMap, CoreResource> {
    private final Logger logger = logger(MethodHandles.lookup().lookupClass());

    public ConfigMapResource() {
        this(ConfigMap.class);
    }

    public ConfigMapResource(final Class<ConfigMap> resourceType) {
        super(resourceType);
    }

    @Override
    public ConfigMap create(final ConfigMap target, final CoreResource resource, final Context<CoreResource> context) {
        try {
            if (resource.getSpec().logManifest()) logger.info("Creating managed resource", Map.of(PIPELINE_ID, String.valueOf(resource.pipelineId()), "manifest", yamlString(target).replaceAll("\n", "\r")));
            var configMap = super.create(target, resource, context);
            return configMap;
        } catch (Exception e) {
            logger.error("Error in creating managed resource", Map.of(PIPELINE_ID, String.valueOf(resource.pipelineId()), ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public ConfigMap update(final ConfigMap actual, final ConfigMap target, final CoreResource resource, final Context<CoreResource> context) {
        try {
            if (resource.getSpec().logManifest()) {
                String actualYaml = yamlString(actual);
                String targetYaml = yamlString(target);
                if (!actualYaml.equals(targetYaml)) logger.info("Updating managed resource with diff", Map.of(PIPELINE_ID, String.valueOf(resource.pipelineId()), "old", actualYaml.replaceAll("\n", "\r"), "new", targetYaml.replaceAll("\n", "\r")));
            }
            var configMap = super.update(actual, target, resource, context);
            return configMap;
        } catch (Exception e) {
            logger.error("Error in updating managed resource", Map.of(PIPELINE_ID, String.valueOf(resource.pipelineId()), ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    protected ConfigMap desired(final CoreResource resource, final Context<CoreResource> context) {
        CoreStatus status;
        try {
            status = resource.getStatus();
            CoreOptions options = getOperatorOptions(resource);
            Map<String, String> defaultLabels = defaultLabels(null, options, resource);
            Map<String, String> configData = getConfigDataMap(status.getPipelineId(), options);
            ConfigMap configMap = getConfigMap(resource, status.getPipelineId(), defaultLabels, configData);
            return configMap;
        } catch (Exception e) {
            logger.error("Error in fetching the desired managed resource", Map.of(PIPELINE_ID, String.valueOf(resource.pipelineId()), ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
            throw new RuntimeException(e);
        }
    }
}