package com.apple.aml.stargate.k8s.boot;

import com.apple.aml.stargate.common.utils.AppConfig;
import com.apple.aml.stargate.common.utils.JsonUtils;
import com.typesafe.config.ConfigRenderOptions;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.http.HttpClient;
import io.fabric8.kubernetes.client.utils.Serialization;
import org.slf4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.apple.aml.stargate.common.constants.CommonConstants.K8sOperator.GROUP;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sOperator.PLURAL;
import static com.apple.aml.stargate.common.constants.CommonConstants.K8sOperator.VERSION;
import static com.apple.aml.stargate.common.utils.HoconUtils.merge;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.PrometheusUtils.initializeMetrics;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static io.javaoperatorsdk.operator.api.reconciler.Constants.WATCH_ALL_NAMESPACES;
import static io.javaoperatorsdk.operator.api.reconciler.Constants.WATCH_CURRENT_NAMESPACE;
import static java.util.Arrays.asList;

@EnableScheduling
@EnableAsync
@SpringBootApplication(scanBasePackages = {"com.apple.aml.stargate", "io.javaoperatorsdk"})
@SuppressWarnings("deprecation")
public class StargateOperator {
    public static void main(final String[] args) throws Exception {
        initializeMetrics();
        Logger logger = logger(MethodHandles.lookup().lookupClass());
        logger.info("Applying latest version of crd!!");
        CustomResourceDefinition crd = Serialization.unmarshal(StargateOperator.class.getClassLoader().getResourceAsStream(String.format("META-INF/fabric8/%s.%s-%s.yml", PLURAL, GROUP, VERSION)));
        new KubernetesClientBuilder().build().apiextensions().v1().customResourceDefinitions().resource(crd).createOrReplace();
        logger.info("CustomResourceDefinition successfully applied!!");
        logger.info("Starting App..");
        Properties overrides = new Properties();
        String namespaces = AppConfig.config().getString("javaoperatorsdk.reconcilers.corereconciler.namespaces");
        if (isBlank(namespaces) || "ALL".equalsIgnoreCase(namespaces)) {
            namespaces = WATCH_ALL_NAMESPACES;
        } else if ("CURRENT".equalsIgnoreCase(namespaces)) {
            namespaces = WATCH_CURRENT_NAMESPACE;
        }
        overrides.put("javaoperatorsdk.reconcilers.corereconciler.namespaces", namespaces.trim());
        Map<Object, Object> configObject = readJsonMap(merge(AppConfig.configObject(), overrides).render(ConfigRenderOptions.concise().setJson(true)));
        for (final String key : asList("os", "line", "sun", "jdk", "path", "file", "java", "user", "awt")) {
            configObject.remove(key);
        }
        String jsonString = JsonUtils.jsonString(configObject);
        System.setProperty("spring.application.json", jsonString);
        final SpringApplication app = new SpringApplication(StargateOperator.class);
        app.run(args);
    }

    @Bean
    @Primary
    public KubernetesClient kubernetesClient(final Optional<HttpClient.Factory> httpClientFactory) {
        KubernetesClientBuilder builder = new KubernetesClientBuilder();
        if (httpClientFactory != null && httpClientFactory.isPresent()) {
            builder = builder.withHttpClientFactory(httpClientFactory.get());
        }
        return builder.build();
    }
}
