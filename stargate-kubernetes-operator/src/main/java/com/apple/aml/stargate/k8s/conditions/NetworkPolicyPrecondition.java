package com.apple.aml.stargate.k8s.conditions;

import com.apple.aml.stargate.k8s.crd.CoreResource;
import com.apple.aml.stargate.k8s.crd.CoreStatus;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.DependentResource;
import io.javaoperatorsdk.operator.processing.dependent.workflow.Condition;

import static com.apple.aml.stargate.common.utils.K8sUtils.STATUS.COMPLETED;

public class NetworkPolicyPrecondition implements Condition<NetworkPolicy, CoreResource> {

    @Override
    public boolean isMet(final DependentResource<NetworkPolicy, CoreResource> dependentResource, final CoreResource primary, final Context<CoreResource> context) {
        CoreStatus status = primary.getStatus();
        return status.getRunNo() > 0 && !COMPLETED.name().equalsIgnoreCase(status.getStatus());
    }
}

