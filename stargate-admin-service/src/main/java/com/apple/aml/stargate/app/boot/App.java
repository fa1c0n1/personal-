package com.apple.aml.stargate.app.boot;

import com.apple.aml.stargate.common.grpc.GrpcServer;
import com.apple.aml.stargate.common.http2.Http2Server;
import com.apple.aml.stargate.common.utils.AppConfig;
import com.apple.aml.stargate.common.utils.JsonUtils;
import com.apple.appeng.aluminum.core.application.annotations.types.StatelessBusinessService;
import com.typesafe.config.ConfigRenderOptions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

import javax.annotation.PostConstruct;
import java.io.Closeable;
import java.util.Map;

import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;
import static java.util.Arrays.asList;

@StatelessBusinessService
@EnableScheduling
@EnableAsync
@SpringBootApplication(scanBasePackages = {"com.apple.aml.stargate"})
public class App implements Closeable {
    @Autowired
    private GrpcServer grpcServer;
    @Autowired
    private Http2Server http2Server;

    public static void main(final String[] args) {
        Map<Object, Object> configObject = readJsonMap(AppConfig.configObject().render(ConfigRenderOptions.concise().setJson(true)));
        for (final String key : asList("os", "line", "sun", "jdk", "path", "file", "java", "user", "awt")) {
            configObject.remove(key); // should have used something like ApplicationContextInitializer/EnvironmentPostProcessor, but that doesn't work always with HOCON format & hence this approach
        }
        String jsonString = JsonUtils.jsonString(configObject);
        System.setProperty("spring.application.json", jsonString);
        final SpringApplication app = new SpringApplication(App.class);
        app.run(args);
    }

    @PostConstruct
    void init() {
        try {
            grpcServer.startServer();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        try {
            http2Server.startServer();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void close() {
    }

}
