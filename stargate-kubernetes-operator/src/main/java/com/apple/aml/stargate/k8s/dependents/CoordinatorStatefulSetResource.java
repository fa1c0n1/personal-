package com.apple.aml.stargate.k8s.dependents;

import com.apple.aml.stargate.common.pojo.CoreOptions;
import com.apple.aml.stargate.k8s.crd.CoreResource;
import com.apple.aml.stargate.k8s.crd.CoreStatus;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.K8sLabelValues.COORDINATOR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.PipelineConstants.PIPELINE_ID;
import static com.apple.aml.stargate.common.utils.JsonUtils.yamlString;
import static com.apple.aml.stargate.common.utils.K8sUtils.STARGATE_MANAGED;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.k8s.utils.ReconcilerUtils.defaultLabels;
import static com.apple.aml.stargate.k8s.utils.ReconcilerUtils.getCoordinatorStatefulSet;
import static com.apple.aml.stargate.k8s.utils.ReconcilerUtils.getOperatorOptions;

@Deprecated
@KubernetesDependent(labelSelector = STARGATE_MANAGED)
public class CoordinatorStatefulSetResource extends CRUDKubernetesDependentResource<StatefulSet, CoreResource> {
    private final Logger logger = logger(MethodHandles.lookup().lookupClass());

    public CoordinatorStatefulSetResource() {
        this(StatefulSet.class);
    }

    public CoordinatorStatefulSetResource(final Class<StatefulSet> resourceType) {
        super(resourceType);
    }

    @Override
    public StatefulSet create(final StatefulSet target, final CoreResource resource, final Context<CoreResource> context) {
        try {
            if (resource.getSpec().logManifest()) logger.info("Creating managed resource", Map.of("manifest", yamlString(target).replaceAll("\n", "\r")));
            var statefulSet = super.create(target, resource, context);
            return statefulSet;
        } catch (Exception e) {
            logger.error("Error in creating managed resource", Map.of(PIPELINE_ID, String.valueOf(resource.pipelineId()), ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public StatefulSet update(final StatefulSet actual, final StatefulSet target, final CoreResource resource, final Context<CoreResource> context) {
        try {
            if (resource.getSpec().logManifest()) {
                String actualYaml = yamlString(actual);
                String targetYaml = yamlString(target);
                if (!actualYaml.equals(targetYaml)) logger.info("Updating managed resource with diff", Map.of(PIPELINE_ID, String.valueOf(resource.pipelineId()), "old", actualYaml.replaceAll("\n", "\r"), "new", targetYaml.replaceAll("\n", "\r")));
            }
            var statefulSet = super.update(actual, target, resource, context);
            return statefulSet;
        } catch (Exception e) {
            logger.error("Error in updating managed resource", Map.of(PIPELINE_ID, String.valueOf(resource.pipelineId()), ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public StatefulSet desired(final CoreResource resource, final Context<CoreResource> context) {
        CoreStatus status = null;
        try {
            status = resource.getStatus();
            CoreOptions options = getOperatorOptions(resource);
            Map<String, String> defaultLabels = defaultLabels(COORDINATOR, options, resource);
            StatefulSet statefulSet = getCoordinatorStatefulSet(resource, status.getPipelineId(), status.getRunNo(), defaultLabels, options, status.getSharedToken());
            return statefulSet;
        } catch (Exception e) {
            logger.error("Error in fetching the desired managed resource", Map.of(PIPELINE_ID, String.valueOf(resource.pipelineId()), ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
            throw new RuntimeException(e);
        }
    }
}
