package com.apple.aml.stargate.pipeline.inject;

import com.apple.aml.stargate.common.options.SnowflakeOptions;
import com.apple.aml.stargate.common.services.SnowflakeService;
import com.zaxxer.hikari.HikariConfig;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static com.apple.aml.stargate.common.constants.CommonConstants.RsaHeaders.PRIVATE_KEY_HEADER_INCLUDES;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.pipelineId;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.jvm.commons.util.Strings.isBlank;

public class SnowflakeServiceHandler extends JdbiServiceHandler implements SnowflakeService, Serializable {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final ConcurrentHashMap<String, SnowflakeServiceHandler> HANDLER_CACHE = new ConcurrentHashMap<>();

    public SnowflakeServiceHandler(final String nodeName, final SnowflakeOptions options) {
        super(nodeName, options);
    }

    public static SnowflakeServiceHandler snowflakeService(final String nodeName, final SnowflakeOptions options) {
        return HANDLER_CACHE.computeIfAbsent(nodeName, n -> new SnowflakeServiceHandler(nodeName, options));
    }

    public static SnowflakeServiceHandler snowflakeService(final String nodeName, final Function<String, SnowflakeServiceHandler> function) {
        return HANDLER_CACHE.computeIfAbsent(nodeName, function);
    }

    @Override
    protected HikariConfig hikariConfig() throws Exception {
        HikariConfig config = super.hikariConfig();
        SnowflakeOptions snowflakeOptions = (SnowflakeOptions) options;
        if (!isBlank(snowflakeOptions.getWarehouse())) config.addDataSourceProperty("warehouse", snowflakeOptions.getWarehouse());
        configurePrivateKey(config, snowflakeOptions);
        return config;
    }

    private void configurePrivateKey(final HikariConfig config, final SnowflakeOptions snowflakeOptions) throws Exception {
        String pemContent = snowflakeOptions.getPrivateKey();
        if (isBlank(pemContent)) return;
        if (!pemContent.contains(PRIVATE_KEY_HEADER_INCLUDES)) pemContent = new String(Base64.getDecoder().decode(pemContent), StandardCharsets.UTF_8);
        Path pemPath = Paths.get(String.format("/tmp/snowflake-%s-%s.pem", pipelineId(), nodeName).toLowerCase());
        if (!pemPath.toFile().exists()) Files.write(pemPath, pemContent.getBytes(StandardCharsets.UTF_8));
        config.addDataSourceProperty("private_key_file", pemPath.toFile().getAbsolutePath());
        if (!isBlank(snowflakeOptions.getPassphrase())) config.addDataSourceProperty("private_key_file_pwd", snowflakeOptions.getPassphrase());
    }
}
