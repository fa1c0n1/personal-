package com.apple.aml.stargate.k8s.conditions;

import com.apple.aml.stargate.k8s.crd.CoreResource;
import com.apple.aml.stargate.k8s.crd.CoreStatus;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.DependentResource;
import io.javaoperatorsdk.operator.processing.dependent.workflow.Condition;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.PipelineConstants.PIPELINE_ID;
import static com.apple.aml.stargate.common.utils.K8sUtils.STATUS.COMPLETED;
import static com.apple.aml.stargate.common.utils.K8sUtils.configMapName;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;

public class CoordinatorJobPrecondition implements Condition<Job, CoreResource> {
    private final Logger logger = logger(MethodHandles.lookup().lookupClass());

    @Override
    public boolean isMet(final DependentResource<Job, CoreResource> dependentResource, final CoreResource primary, final Context<CoreResource> context) {
        CoreStatus status = primary.getStatus();
        if (status.getRunNo() <= 0 || COMPLETED.name().equalsIgnoreCase(status.getStatus())) return false;
        try {
            ConfigMap cm = context.getClient().configMaps().inNamespace(primary.getMetadata().getNamespace()).withName(configMapName(primary.pipelineId())).get();
            return cm != null && cm.getData() != null && !cm.getData().isEmpty();
        } catch (Exception e) {
            logger.error("Error in fetching config map details", Map.of(PIPELINE_ID, String.valueOf(primary.pipelineId()), ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
            return false;
        }
    }
}

