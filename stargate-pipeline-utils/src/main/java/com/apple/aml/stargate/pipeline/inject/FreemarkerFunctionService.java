package com.apple.aml.stargate.pipeline.inject;

import com.apple.aml.stargate.common.services.DhariService;
import com.apple.aml.stargate.common.services.ErrorService;
import com.apple.aml.stargate.common.services.JdbiService;
import com.apple.aml.stargate.common.services.NodeService;
import com.apple.aml.stargate.common.services.PrometheusService;
import com.apple.aml.stargate.common.services.RestProxyService;
import com.apple.aml.stargate.common.services.RestService;
import com.apple.aml.stargate.common.services.ShuriOfsService;
import com.apple.aml.stargate.common.services.SnowflakeService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.function.Function;

import static com.apple.aml.stargate.common.constants.PipelineConstants.RUNTIME_LOGGER_NODE_PREFIX;

@Getter
public class FreemarkerFunctionService implements Function<Object, Object> {
    @Inject
    @Named("context")
    private Map context;
    private Logger loggingService;
    @Inject
    private NodeService nodeService;
    @Inject
    private ErrorService errorService;
    @Inject
    private PrometheusService metricsService;
    @Inject(optional = true)
    @Nullable
    private DhariService dhariService;
    @Inject(optional = true)
    @Nullable
    private SnowflakeService snowflakeService;
    @Inject(optional = true)
    @Nullable
    private ShuriOfsService attributeService;
    @Inject(optional = true)
    @Nullable
    private RestService restService;
    @Inject(optional = true)
    @Nullable
    private RestProxyService restProxyService;
    @Inject(optional = true)
    @Nullable
    private JdbiService jdbiService;

    @Inject
    public FreemarkerFunctionService(@Named("nodeName") String nodeName) {
        loggingService = LoggerFactory.getLogger(String.format("%s.%s", RUNTIME_LOGGER_NODE_PREFIX, nodeName));
    }

    public Logger logger() {
        return loggingService;
    }

    public NodeService node() {
        return nodeService;
    }

    public ErrorService errors() {
        return errorService;
    }

    public PrometheusService metrics() {
        return metricsService;
    }

    public DhariService dhari() {
        return dhariService;
    }

    public SnowflakeService snowflake() {
        return snowflakeService;
    }

    public ShuriOfsService attributes() {
        return attributeService;
    }

    public ShuriOfsService shuriofs() {
        return attributeService;
    }

    public RestService rest() {
        return restService;
    }

    public RestService rest(final String serviceId) {
        return restProxyService.service(serviceId);
    }

    public RestProxyService restproxy() {
        return restProxyService;
    }

    public JdbiService jdbc() {
        return jdbiService;
    }

    public JdbiService jdbi() {
        return jdbiService;
    }

    @Override
    public Object apply(final Object o) {
        return o;
    }
}