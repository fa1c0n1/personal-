package com.apple.aml.stargate.common.grpc;

import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.lang.invoke.MethodHandles;

import static com.apple.aml.stargate.common.utils.AppConfig.config;
import static com.apple.aml.stargate.common.utils.AppConfig.configPrefix;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;

@Component
@Lazy
public class GrpcServer {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    @Lazy
    @Autowired(required = false)
    private BindableService bindableService;
    @Lazy
    @Autowired(required = false)
    private ServerInterceptor interceptor;
    private Server server;

    @Async
    public void startServer() throws Exception {
        String configPrefix = configPrefix();
        Config config = config();
        String enabledConfig = String.format("%s.grpc.enabled", configPrefix);
        if ((!config.hasPath(enabledConfig) || config.getBoolean(enabledConfig)) && bindableService != null && interceptor != null) {
            int serverPort = config.getInt(String.format("%s.grpc.port", configPrefix));
            server = ServerBuilder.forPort(serverPort).addService(bindableService).intercept(interceptor).build();
            LOGGER.info("Starting grpc server at port {}", serverPort);
            server.start();
        } else {
            LOGGER.debug("GrpcServer is disabled");
        }
    }

    public Server server() {
        return server;
    }
}
