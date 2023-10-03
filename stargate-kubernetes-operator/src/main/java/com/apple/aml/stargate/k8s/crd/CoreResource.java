package com.apple.aml.stargate.k8s.crd;

import com.apple.aml.stargate.common.pojo.CoreOptions;
import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Kind;
import io.fabric8.kubernetes.model.annotation.Plural;
import io.fabric8.kubernetes.model.annotation.ShortNames;
import io.fabric8.kubernetes.model.annotation.Singular;
import io.fabric8.kubernetes.model.annotation.Version;

import static com.apple.aml.stargate.common.constants.CommonConstants.K8sOperator.GROUP;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sOperator.KIND;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sOperator.PLURAL;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sOperator.SHORT_NAME_1;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sOperator.SHORT_NAME_2;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sOperator.SINGULAR;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sOperator.VERSION;
import static com.apple.aml.stargate.common.constants.CommonConstants.STARGATE;
import static com.apple.aml.stargate.common.utils.K8sUtils.STATUS.NOT_STARTED;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static org.apache.commons.lang3.builder.ToStringBuilder.reflectionToString;
import static org.apache.commons.lang3.builder.ToStringStyle.JSON_STYLE;

@Group(GROUP)
@Version(VERSION)
@ShortNames({SHORT_NAME_1, SHORT_NAME_2, STARGATE})
@Singular(SINGULAR)
@Plural(PLURAL)
@Kind(KIND)
public class CoreResource extends CustomResource<CoreOptions, CoreStatus> implements Namespaced {

    @Override
    protected CoreStatus initStatus() {
        CoreStatus status = new CoreStatus();
        status.setRunNo(-1);
        status.setStatus(NOT_STARTED.name());
        return status;
    }

    @Override
    public String toString() {
        return reflectionToString(this, JSON_STYLE);
    }

    public String pipelineId() {
        return (isBlank(this.getSpec().getPipelineId()) ? this.getMetadata().getName() : this.getSpec().getPipelineId()).toLowerCase().trim();
    }

    public boolean sameRequestId() {
        if (status.getRequestId() == null) {
            return getSpec().getRequestId() == null;
        } else return status.getRequestId().equals(getSpec().getRequestId());
    }
}