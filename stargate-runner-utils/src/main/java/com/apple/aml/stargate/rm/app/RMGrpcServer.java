package com.apple.aml.stargate.rm.app;

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

import static com.apple.aml.stargate.common.constants.CommonConstants.K8sPorts.GRPC;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;

@Component
@Lazy
public class RMGrpcServer {
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
        server = ServerBuilder.forPort(GRPC).addService(bindableService).intercept(interceptor).build();
        LOGGER.info("Starting resource manager grpc server at port {}", GRPC);
        server.start();
    }

    public Server server() {
        return server;
    }
}
