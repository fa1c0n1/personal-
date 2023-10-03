package com.apple.aml.stargate.spark.facade.boot;

import com.apple.aml.stargate.common.utils.AppConfig;
import com.apple.aml.stargate.common.utils.JsonUtils;
import com.typesafe.config.ConfigRenderOptions;
import net.devh.boot.grpc.server.autoconfigure.GrpcServerSecurityAutoConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;
import static java.util.Arrays.asList;

@EnableScheduling
@EnableAsync
@SpringBootApplication(scanBasePackages = {"com.apple.aml.stargate"}, exclude = {GrpcServerSecurityAutoConfiguration.class})
public class SparkApp implements Closeable {

    public static void main(final String[] args) {
        Map<Object, Object> configObject = readJsonMap(AppConfig.configObject().render(ConfigRenderOptions.concise().setJson(true)));
        for (final String key : asList("os", "line", "sun", "jdk", "path", "file", "java", "user", "awt")) {
            configObject.remove(key); // should have used something like ApplicationContextInitializer/EnvironmentPostProcessor, but that doesn't work always with HOCON format & hence this approach
        }
        String jsonString = JsonUtils.jsonString(configObject);
        System.setProperty("spring.application.json", jsonString);
        final SpringApplication app = new SpringApplication(SparkApp.class);
        app.run(args);
    }

    @Override
    public void close() throws IOException {

    }
}
