package com.apple.aml.stargate.pipeline.inject;

import com.apple.aml.stargate.common.options.AttributesOptions;
import com.apple.aml.stargate.common.options.JavaFunctionOptions;
import com.apple.aml.stargate.common.services.AttributeService;
import com.apple.aml.stargate.common.services.DhariService;
import com.apple.aml.stargate.common.services.ErrorService;
import com.apple.aml.stargate.common.services.JdbcService;
import com.apple.aml.stargate.common.services.JdbiService;
import com.apple.aml.stargate.common.services.LookupService;
import com.apple.aml.stargate.common.services.MetricsService;
import com.apple.aml.stargate.common.services.NodeService;
import com.apple.aml.stargate.common.services.PrometheusService;
import com.apple.aml.stargate.common.services.RestProxyService;
import com.apple.aml.stargate.common.services.RestService;
import com.apple.aml.stargate.common.services.ShuriOfsService;
import com.apple.aml.stargate.common.services.SnowflakeService;
import com.apple.aml.stargate.common.services.TTLAttributeService;
import com.apple.aml.stargate.common.utils.ContextHandler;
import com.apple.aml.stargate.common.web.clients.RestClient;
import com.apple.aml.stargate.common.web.clients.RestProxyClient;
import com.apple.jvm.commons.metrics.MetricsEngine;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.matcher.Matchers;
import com.google.inject.name.Names;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.inject.JdbiServiceHandler.jdbiService;
import static com.apple.aml.stargate.pipeline.inject.SnowflakeServiceHandler.snowflakeService;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;

@Data
@NoArgsConstructor(force = true)
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class GuiceModule extends AbstractModule {
    Injector injector;
    @NonNull
    private String nodeName;
    @NonNull
    private Class singletonClass;
    @NonNull
    private JavaFunctionOptions options;
    @NonNull
    private PrometheusService metricsService;
    @NonNull
    private NodeService nodeService;
    @NonNull
    private ErrorService errorService;
    @NonNull
    private DhariService dhariService;
    @NonNull
    private Map<String, Object> context;

    @Override
    @SuppressWarnings("unchecked")
    @SneakyThrows
    protected void configure() {
        super.configure();
        bind(singletonClass).in(Singleton.class);
        bind(JavaFunctionOptions.class).toInstance(options);
        bind(MetricsEngine.class).toInstance(metricsService);
        bind(MetricsService.class).toInstance(metricsService);
        bind(PrometheusService.class).toInstance(metricsService);
        bind(NodeService.class).toInstance(nodeService);
        bind(ErrorService.class).toInstance(errorService);
        bind(DhariService.class).toInstance(dhariService);
        ShuriOfsService attributeService = getAttributeService(nodeName, options.getAttributesOptions());
        bind(ShuriOfsService.class).toProvider(() -> attributeService);
        bind(AttributeService.class).toProvider(() -> attributeService);
        bind(TTLAttributeService.class).toProvider(() -> attributeService);
        bind(LookupService.class).toProvider(() -> attributeService);
        bind(SnowflakeService.class).toProvider(() -> options.getSnowflakeOptions() == null ? null : snowflakeService(nodeName, options.getSnowflakeOptions()));
        bind(JdbcService.class).toProvider(() -> options.getJdbcOptions() == null ? null : jdbiService(nodeName, options.getJdbcOptions()));
        bind(JdbiService.class).toProvider(() -> options.getJdbcOptions() == null ? null : jdbiService(nodeName, options.getJdbcOptions()));
        bind(RestService.class).toProvider(() -> options.getRestOptions() == null ? null : new RestClient(options.getRestOptions(), logger(singletonClass), Map.of(NODE_NAME, nodeName)));
        bind(RestProxyService.class).toProvider(() -> options.getRestServiceOptions() == null ? null : new RestProxyClient(options.getRestServiceOptions(), logger(singletonClass), Map.of(NODE_NAME, nodeName)));
        bind(Map.class).annotatedWith(Names.named("context")).toInstance(context);
        bind(new TypeLiteral<Map<String, Object>>() {
        }).annotatedWith(Names.named("context")).toInstance(context);
        bind(new TypeLiteral<Map<String, ?>>() {
        }).annotatedWith(Names.named("context")).toInstance(context);
        bind(String.class).annotatedWith(Names.named("nodeName")).toInstance(nodeName);
        bindInterceptor(Matchers.any(), Matchers.annotatedWith(MetricsService.RecordHistogram.class), new RecordHistogramInterceptor());
    }

    @SuppressWarnings("unchecked")
    private static ShuriOfsService getAttributeService(final String nodeName, final AttributesOptions options) throws ClassNotFoundException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        if (options == null) return null;
        Class serviceClass = Class.forName("com.apple.aml.stargate.connector.athena.attributes.AthenaAttributeService");
        ShuriOfsService attributeService = (ShuriOfsService) serviceClass.getMethod("attributeService", String.class, AttributesOptions.class).invoke(null, nodeName, options);
        return attributeService;
    }

    public static class RecordHistogramInterceptor implements MethodInterceptor {
        @Override
        public Object invoke(final MethodInvocation invocation) throws Throwable {
            long startTime = System.nanoTime();
            ContextHandler.Context ctx = ContextHandler.ctx();
            Method method = invocation.getMethod();
            String stage = String.format("method:%s:%s", method.getDeclaringClass().getName(), method.getName());
            try {
                return invocation.proceed();
            } finally {
                histogramDuration(ctx.getNodeName(), ctx.getNodeType(), ctx.getSchema().getFullName(), stage).observe((System.nanoTime() - startTime) / 1000000.0);
            }
        }
    }
}
