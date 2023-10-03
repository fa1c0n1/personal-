package com.apple.aml.stargate.cache.utils;

import com.apple.aml.stargate.common.utils.AppConfig;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.slf4j.Logger;

import javax.naming.ConfigurationException;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.atomic.AtomicReference;

import static com.apple.aml.stargate.common.utils.JsonUtils.dropSensitiveKeys;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonToYaml;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.typesafe.config.ConfigRenderOptions.concise;

public class RedisUtils {

    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    private static final AtomicReference<RedissonClient> redisClientRef = new AtomicReference<>();

    private RedisUtils() {
    }

    public static RedissonClient initRedisClient() throws Exception {
        return initRedisClient(redisClientConfig());
    }

    public static RedissonClient initRedisClient(Config config) {
        redisClientRef.compareAndSet(null, Redisson.create(config));
        return redisClientRef.get();
    }

    public static Config redisClientConfig() throws Exception {
        com.typesafe.config.Config appConfig = AppConfig.config();
        String redisMode = appConfig.hasPath("redis.mode") ? appConfig.getString("redis.mode") : "replicated";
        String redisConfigPath = "redis.".concat(redisMode);
        if (!appConfig.hasPath(redisConfigPath))
            throw new ConfigurationException("Missing redis config - " + redisConfigPath);

        String jsonConfig = appConfig.getConfig(redisConfigPath).root().render(concise());
        //LOGGER.debug("redis json config {}", dropSensitiveKeys(jsonConfig));
        String yamlConfig = jsonToYaml(jsonConfig);
        LOGGER.debug("redis yaml config {}", dropSensitiveKeys(yamlConfig));
        return Config.fromYAML(yamlConfig);
    }

    public void shutdownRedisClient() {
        if (redisClientRef.compareAndSet(redisClientRef.get(), null)) {
            redisClientRef.get().shutdown();
        }
    }
}
