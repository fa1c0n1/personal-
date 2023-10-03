package com.apple.aml.stargate.common.web.clients;

import com.apple.aml.stargate.common.options.HttpInvokerOptions;
import com.apple.aml.stargate.common.services.RestProxyService;
import com.apple.aml.stargate.common.services.RestService;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.apple.aml.stargate.common.utils.ClassUtils.getAs;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;

public class RestProxyClient implements RestProxyService, Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private final Map<String, Object> options;
    private final Logger logger;
    private final Map<String, Object> logMap;
    private ConcurrentHashMap<String, RestClient> clientMap = new ConcurrentHashMap<>();

    public RestProxyClient(final Map<String, Object> options, final Logger logger, final Map<String, Object> logMap) {
        this.options = options;
        this.logger = logger == null ? LOGGER : logger;
        this.logMap = logMap == null ? Map.of() : logMap;
    }

    @Override
    public <O> O invoke(final String serviceId, final Object payload, final Map<String, String> headers, final Class<O> returnType, final boolean throwOnError) {
        return service(serviceId).invoke(payload, headers, returnType, throwOnError);
    }

    @Override
    public RestService service(final String serviceId) {
        return clientMap.computeIfAbsent(serviceId, s -> new RestClient(getAs(options.get(s), HttpInvokerOptions.class), this.logger, this.logMap));
    }
}
