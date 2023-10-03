package com.apple.aml.stargate.connector.athena.attributes;

import com.apple.aml.stargate.beam.sdk.ts.FreemarkerEvaluator;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.AttributesOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;

import static com.apple.aml.stargate.common.utils.ClassUtils.duplicate;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.connector.athena.attributes.AthenaAttributeService.attributeService;

public class AttributesOperations extends FreemarkerEvaluator implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private AttributesOptions options;
    private String nodeName;

    @Override
    public void initTransform(final Pipeline pipeline, final StargateNode node) throws Exception {
        this.nodeName = node.getName();
        super.initTransform(pipeline, node);
        this.options = duplicate(node.getConfig(), AttributesOptions.class);
    }

    @Override
    public void initWrite(final Pipeline pipeline, final StargateNode node) throws Exception {
        this.nodeName = node.getName();
        super.initWrite(pipeline, node);
        this.options = duplicate(node.getConfig(), AttributesOptions.class);
    }

    @Override
    public Pair<String, Object> bean() {
        return Pair.of("client", attributeService(nodeName, options));
    }
}
