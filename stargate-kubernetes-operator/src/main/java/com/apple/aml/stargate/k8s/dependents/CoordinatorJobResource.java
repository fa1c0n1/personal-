package com.apple.aml.stargate.k8s.dependents;

import com.apple.aml.stargate.common.pojo.CoreOptions;
import com.apple.aml.stargate.k8s.crd.CoreResource;
import com.apple.aml.stargate.k8s.crd.CoreStatus;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static com.apple.aml.stargate.common.constants.CommonConstants.K8sLabelValues.COORDINATOR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_OPERATOR_JOB_DELETE_WAIT_TIME;
import static com.apple.aml.stargate.common.constants.PipelineConstants.PIPELINE_ID;
import static com.apple.aml.stargate.common.utils.AppConfig.env;
import static com.apple.aml.stargate.common.utils.JsonUtils.yamlString;
import static com.apple.aml.stargate.common.utils.K8sUtils.STARGATE_MANAGED;
import static com.apple.aml.stargate.common.utils.K8sUtils.coordinatorName;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.k8s.utils.ReconcilerUtils.defaultLabels;
import static com.apple.aml.stargate.k8s.utils.ReconcilerUtils.getCoordinatorJob;
import static com.apple.aml.stargate.k8s.utils.ReconcilerUtils.getOperatorOptions;
import static java.lang.Integer.parseInt;

@KubernetesDependent(labelSelector = STARGATE_MANAGED)
public class CoordinatorJobResource extends CRUDKubernetesDependentResource<Job, CoreResource> {
    private final Logger logger = logger(MethodHandles.lookup().lookupClass());

    public CoordinatorJobResource() {
        this(Job.class);
    }

    public CoordinatorJobResource(final Class<Job> resourceType) {
        super(resourceType);
    }

    @Override
    public Job create(final Job target, final CoreResource resource, final Context<CoreResource> context) {
        try {
            if (resource.getSpec().logManifest()) logger.info("Creating managed resource", Map.of("manifest", yamlString(target).replaceAll("\n", "\r")));
            var job = super.create(target, resource, context);
            return job;
        } catch (Exception e) {
            logger.error("Error in creating managed resource", Map.of(PIPELINE_ID, String.valueOf(resource.pipelineId()), ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Job update(final Job actual, final Job target, final CoreResource resource, final Context<CoreResource> context) {
        try {
            if (resource.getSpec().logManifest()) {
                String actualYaml = yamlString(actual);
                String targetYaml = yamlString(target);
                if (!actualYaml.equals(targetYaml)) logger.info("Updating managed resource with diff", Map.of(PIPELINE_ID, String.valueOf(resource.pipelineId()), "old", actualYaml.replaceAll("\n", "\r"), "new", targetYaml.replaceAll("\n", "\r")));
            }
            Job job;
            if (actual != null && client.batch().v1().jobs().inNamespace(resource.getMetadata().getNamespace()).withName(coordinatorName(resource.pipelineId())).get() != null) {
                job = client.batch().v1().jobs().inNamespace(resource.getMetadata().getNamespace()).withName(coordinatorName(resource.pipelineId())).patch(target);
            } else {
                job = super.update(actual, target, resource, context);
            }
            return job;
        } catch (Exception e) {
            try {
                client.batch().v1().jobs().inNamespace(resource.getMetadata().getNamespace()).withName(coordinatorName(resource.pipelineId())).delete();
                client.batch().v1().jobs().inNamespace(resource.getMetadata().getNamespace()).withName(coordinatorName(resource.pipelineId())).waitUntilCondition(Objects::isNull, parseInt(env(STARGATE_OPERATOR_JOB_DELETE_WAIT_TIME, "30")), TimeUnit.SECONDS);
                return create(target, resource, context);
            } catch (Exception ce) {
                logger.warn("Error in delete & recreate managed resource", Map.of(PIPELINE_ID, String.valueOf(resource.pipelineId()), ERROR_MESSAGE, String.valueOf(ce.getMessage())), ce);
                logger.error("Error in updating managed resource; Tried manual delete too", Map.of(PIPELINE_ID, String.valueOf(resource.pipelineId()), ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public Job desired(final CoreResource resource, final Context<CoreResource> context) {
        CoreStatus status = null;
        try {
            status = resource.getStatus();
            CoreOptions options = getOperatorOptions(resource);
            Map<String, String> defaultLabels = defaultLabels(COORDINATOR, options, resource);
            Job job = getCoordinatorJob(resource, status.getPipelineId(), status.getRunNo(), defaultLabels, options, status.getSharedToken());
            return job;
        } catch (Exception e) {
            logger.error("Error in fetching the desired managed resource", Map.of(PIPELINE_ID, String.valueOf(resource.pipelineId()), ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
            throw new RuntimeException(e);
        }
    }
}
