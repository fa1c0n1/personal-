package com.apple.aml.stargate.common.spring.config;

import org.springframework.boot.web.codec.CodecCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.codec.ServerCodecConfigurer;
import org.springframework.web.reactive.config.WebFluxConfigurer;

import static com.apple.aml.stargate.common.utils.AppConfig.env;

@Configuration
public class WebfluxConfig implements WebFluxConfigurer {

    @Override
    public void configureHttpMessageCodecs(final ServerCodecConfigurer configurer) {
        configurer.defaultCodecs().maxInMemorySize(maxInMemorySize());
    }

    private int maxInMemorySize() {
        return Integer.parseInt(env("APP_SPRING_MAX_MEMORY_SIZE", String.valueOf(32 * 1024 * 1024)).replaceAll("'", "").replaceAll("\"", "").trim());
    }

    @Bean
    public CodecCustomizer codecCustomizer() {
        return configurer -> configurer.defaultCodecs().maxInMemorySize(maxInMemorySize());
    }
}
