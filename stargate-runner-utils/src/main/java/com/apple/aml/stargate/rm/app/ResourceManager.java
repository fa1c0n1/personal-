package com.apple.aml.stargate.rm.app;

import com.apple.aml.stargate.common.utils.AppConfig;
import com.apple.aml.stargate.common.utils.JsonUtils;
import com.typesafe.config.ConfigRenderOptions;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

import javax.annotation.PostConstruct;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_LOCAL_RM_URI;
import static com.apple.aml.stargate.common.utils.AppConfig.config;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.NetworkUtils.isPortAvailable;
import static com.apple.aml.stargate.common.utils.NetworkUtils.nextAvailablePort;
import static com.apple.aml.stargate.common.utils.PrometheusUtils.initializeMetrics;
import static java.util.Arrays.asList;

@EnableScheduling
@EnableAsync
@SpringBootApplication(scanBasePackages = {"com.apple.aml.stargate"})
public class ResourceManager {
    private static final AtomicReference<Boolean> initialized = new AtomicReference<>();
    @Autowired
    private RMGrpcServer grpcServer;

    public static void startResourceManager() {
        Logger logger = logger(MethodHandles.lookup().lookupClass());
        Thread thread = new Thread(() -> {
            try {
                initializeMetrics();
                initialized.set(true);
                int port = config().getInt("stargate.rm.port");
                if (!isPortAvailable(port)) {
                    if (config().getBoolean("stargate.rm.useAvailablePort")) {
                        port = nextAvailablePort(port);
                    } else {
                        throw new IllegalStateException(String.format("Can not start resource manager. Port %d is not available!!", port));
                    }
                }
                Map<Object, Object> configObject = readJsonMap(AppConfig.configObject().render(ConfigRenderOptions.concise().setJson(true)));
                for (final String key : asList("os", "line", "sun", "jdk", "path", "file", "java", "user", "awt")) {
                    configObject.remove(key);
                }
                String jsonString = JsonUtils.jsonString(configObject);
                System.setProperty("spring.application.json", jsonString);
                final SpringApplication app = new SpringApplication(ResourceManager.class);
                logger.info("Running resource manager at port : {}", port);
                System.setProperty(STARGATE_LOCAL_RM_URI, String.format("http://localhost:%d", port));
                app.setDefaultProperties(Map.of("server.address", "0.0.0.0", "server.port", String.valueOf(port)));
                app.run();
            } catch (Exception e) {
                logger.error("Could not start resource manager", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
                throw new RuntimeException(e);
            }
        });
        thread.setDaemon(true);
        if (initialized.get() == null) {
            thread.run();
        }
    }

    @PostConstruct
    void init() {
        try {
            grpcServer.startServer();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
