package com.apple.aml.stargate.runners.spark.plugin;

import com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils;
import com.apple.aml.stargate.runners.spark.utils.SparkUtils;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkEnv;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.ExecutorPlugin;
import org.apache.spark.api.plugin.PluginContext;
import org.apache.spark.api.plugin.SparkPlugin;
import org.apache.spark.deploy.SparkHadoopUtil;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.common.constants.CommonConstants.DEFAULT_DELIMITER;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.REDACTED_STRING;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_ADDITIONAL_HADOOP_CONF_DIR;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_SHARED_DIRECTORY;
import static com.apple.aml.stargate.common.constants.PipelineConstants.SparkProperties.SPARK_HADOOP_PROP_PREFIX;
import static com.apple.aml.stargate.common.constants.PipelineConstants.SparkProperties.SPARK_PROP_PREFIX;
import static com.apple.aml.stargate.common.constants.PipelineConstants.SparkProperties.STARGATE_SPARK_EXECUTOR_ENV_PREFIX;
import static com.apple.aml.stargate.common.constants.PipelineConstants.SparkProperties.STARGATE_SPARK_EXECUTOR_SYS_PREFIX;
import static com.apple.aml.stargate.common.constants.PipelineConstants.SparkProperties.STARGATE_SPARK_PROP_KERBEROS_KEYTAB;
import static com.apple.aml.stargate.common.constants.PipelineConstants.SparkProperties.STARGATE_SPARK_PROP_KERBEROS_KRB5;
import static com.apple.aml.stargate.common.constants.PipelineConstants.SparkProperties.STARGATE_SPARK_PROP_KERBEROS_PRINCIPAL;
import static com.apple.aml.stargate.common.constants.PipelineConstants.SparkProperties.STARGATE_SPARK_PROP_PREFIX;
import static com.apple.aml.stargate.common.constants.PipelineConstants.SparkProperties.STARGATE_SPARK_PROP_SHARED_DIR_PATH;
import static com.apple.aml.stargate.common.constants.PipelineConstants.SparkProperties.STARGATE_SPARK_PROP_SHARED_FILES;
import static com.apple.aml.stargate.common.constants.PipelineConstants.SparkProperties.STARGATE_SPARK_PROP_SHARED_FILES_COPY;
import static com.apple.aml.stargate.common.utils.AppConfig.env;
import static com.apple.aml.stargate.common.utils.JsonUtils.isRedactable;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.io.hadoop.HdfsSharedConfiguration.SYS_PROP_KRB5_CONF;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.getHadoopConfigReference;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.setHadoopRuntimeConf;
import static com.apple.aml.stargate.rm.app.ResourceManager.startResourceManager;
import static com.apple.aml.stargate.runners.spark.utils.SparkUtils.getAdditionalSparkConf;

public class StartupPlugin implements SparkPlugin {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    @Override
    public DriverPlugin driverPlugin() {
        return new DriverPlugin() {
            @SneakyThrows
            @Override
            public Map<String, String> init(final SparkContext context, final PluginContext ctx) {
                LOGGER.debug("Invoking driverPlugin", Map.of("type", TYPE.driver));
//                startPrometheusServerThread(new AtomicReference<>()); // for future reference
                startResourceManager();
                SparkUtils.setSparkRuntimeConf(ctx.conf());
                Map<String, String> props = extractProperties(TYPE.driver, STARGATE_SPARK_PROP_PREFIX);
                SparkConf sparkConf = context.conf();
                for (Map.Entry<String, String> entry : props.entrySet()) {
                    sparkConf.set(entry.getKey(), entry.getValue());
                }
                props.keySet().stream().filter(x -> x.startsWith(STARGATE_SPARK_EXECUTOR_ENV_PREFIX)).forEach(x -> sparkConf.setExecutorEnv(x.substring(STARGATE_SPARK_EXECUTOR_ENV_PREFIX.length()), props.get(x)));
                props.keySet().stream().filter(x -> x.startsWith(STARGATE_SPARK_EXECUTOR_SYS_PREFIX)).forEach(x -> sparkConf.setExecutorEnv(x.substring(STARGATE_SPARK_EXECUTOR_SYS_PREFIX.length()), props.get(x)));
                SparkConf additionalConf = getAdditionalSparkConf();
                if (additionalConf != null) {
                    Arrays.stream(additionalConf.getAll()).forEach(x -> sparkConf.set(x._1, x._2));
                }
                logPluginContext(TYPE.driver, ctx);
                if (sparkConf.getBoolean(STARGATE_SPARK_PROP_SHARED_FILES_COPY, true)) {
                    copySharedFiles(TYPE.driver, props, context, ctx);
                }
                performKerberosLogin(TYPE.driver, props, ctx);
                return Map.of();
            }

            @Override
            public Object receive(final Object message) throws Exception {
                if (!(message instanceof RpcSharedFileRequest)) {
                    return null;
                }
                RpcSharedFileRequest request = (RpcSharedFileRequest) message;
                String sharedDir = env(STARGATE_SHARED_DIRECTORY, null);
                Path requestedPath = Path.of(sharedDir, request.fileName);
                LOGGER.debug("Received request for shared file", Map.of("requestedPath", requestedPath));
                byte[] content = Files.readAllBytes(requestedPath);
                return RpcSharedFileResponse.sharedFileResponse(request.fileName, content);
            }
        };
    }

    private Map<String, String> extractProperties(final TYPE type, final String prefix) throws IOException {
        Properties props = System.getProperties();
        if (props == null || props.isEmpty()) {
            return new HashMap<>();
        }
        Map<String, String> properties = new HashMap<>();
        for (Map.Entry<Object, Object> entry : props.entrySet()) {
            String key = String.valueOf(entry.getKey());
            if (!key.startsWith(prefix)) {
                continue;
            }
            properties.put(key, String.valueOf(entry.getValue()));
        }
        return properties;
    }

    private void logPluginContext(final TYPE type, final PluginContext ctx) {
        Arrays.stream(ctx.conf().getAll()).forEach(x -> LOGGER.debug("spark config for {}", type, Map.of("key", String.valueOf(x._1), "value", isRedactable(x._1) ? REDACTED_STRING : String.valueOf(x._2))));
    }

    private void copySharedFiles(final TYPE type, final Map<String, String> properties, final SparkContext context, final PluginContext ctx) throws Exception {
        String sharedDir = properties.get(STARGATE_SPARK_PROP_SHARED_DIR_PATH);
        if (sharedDir == null) {
            LOGGER.debug("Could not find sharedDir config. Will default to environment variable", Map.of("type", type));
            sharedDir = env(STARGATE_SHARED_DIRECTORY, null);
        }
        if (sharedDir == null) {
            return;
        }
        Path sharedDirPath = Paths.get(sharedDir);
        File sharedDirectory = sharedDirPath.toFile();
        if (!sharedDirectory.exists()) {
            if (!sharedDirectory.mkdirs()) {
                LOGGER.warn("Could not find sharedDirectory and could not create empty dir either!!", Map.of("sharedDirectory", sharedDirectory.getAbsolutePath()));
            }
        } else if (!sharedDirectory.isDirectory()) {
            LOGGER.warn("sharedDirectory should be a valid directory and not a file!!", Map.of("sharedDirectory", sharedDirectory.getAbsolutePath()));
            return;
        }
        String sharedFiles = properties.get(STARGATE_SPARK_PROP_SHARED_FILES);
        if (sharedFiles != null && !sharedFiles.isBlank()) {
            LOGGER.debug("Copying shared files", Map.of("type", type, "sharedDir", sharedDir, "sharedFiles", sharedFiles));
            for (final String fileName : sharedFiles.split(DEFAULT_DELIMITER)) {
                Path path = Path.of(sharedDir, fileName);
                if (path.toFile().exists()) {
                    continue;
                }
                LOGGER.debug("Shared file does not exist. Will try to copy now from Spark", Map.of("type", type, "sharedDir", sharedDir, "fileName", fileName, "targetPath", path));
                RpcSharedFileResponse response = (RpcSharedFileResponse) ctx.ask(RpcSharedFileRequest.sharedFileRequest(fileName));
                Files.write(path, response.content);
                LOGGER.debug("Copied shared file from Spark successfully via rpc", Map.of("type", type, "sharedDir", sharedDir, "fileName", fileName, "targetPath", path));
            }
        } else {
            LOGGER.debug("sharedFiles is empty. Will not copy anything", Map.of("type", type));
        }
    }

    @SuppressWarnings("unchecked")
    private void performKerberosLogin(final TYPE type, final Map<String, String> properties, final PluginContext ctx) throws Exception {
        try {
            if (!properties.containsKey(STARGATE_SPARK_PROP_KERBEROS_PRINCIPAL) || !properties.containsKey(STARGATE_SPARK_PROP_KERBEROS_KEYTAB)) {
                return;
            }
            String principal = properties.get(STARGATE_SPARK_PROP_KERBEROS_PRINCIPAL);
            String keytab = properties.get(STARGATE_SPARK_PROP_KERBEROS_KEYTAB);
            String krb5 = properties.get(STARGATE_SPARK_PROP_KERBEROS_KRB5);
            LOGGER.debug("Processing HDFS Login via Spark Plugin", Map.of("type", type, "principal", principal, "keytab", keytab, "krb5", krb5));
            System.setProperty(SYS_PROP_KRB5_CONF, krb5);
            Configuration config = SparkHadoopUtil.get().conf();
            Triple<Configuration, Map<String, String>, Set<String>> beamConfig = getHadoopConfigReference();
            beamConfig.getMiddle().forEach((k, v) -> config.set(k, v));
            Arrays.stream(ctx.conf().getAll()).filter(x -> String.valueOf(x._1).startsWith(SPARK_HADOOP_PROP_PREFIX)).forEach(x -> config.set(String.valueOf(x._1).substring(SPARK_HADOOP_PROP_PREFIX.length()), String.valueOf(x._2)));
            String sharedFiles = properties.get(STARGATE_SPARK_PROP_SHARED_FILES);
            if (sharedFiles != null && !sharedFiles.isBlank()) {
                String sharedDir = properties.get(STARGATE_SPARK_PROP_SHARED_DIR_PATH);
                if (sharedDir == null) {
                    sharedDir = env(STARGATE_SHARED_DIRECTORY, null);
                }
                String sharedDirectory = sharedDir;
                List<String> configFiles = Arrays.stream(sharedFiles.split(DEFAULT_DELIMITER)).filter(x -> x.endsWith(".xml")).map(x -> sharedDirectory + "/" + x).collect(Collectors.toList());
                for (final String resource : configFiles) {
                    config.addResource(new org.apache.hadoop.fs.Path("file://" + resource));
                }
                String additionalPath = env(STARGATE_ADDITIONAL_HADOOP_CONF_DIR, null);
                if (additionalPath != null) {
                    for (final String resource : configFiles) {
                        Path source = Path.of(resource);
                        Files.copy(source, Path.of(additionalPath, source.toFile().getName()));
                    }
                }
            }
            setHadoopRuntimeConf(config);
            PipelineUtils.performKerberosLogin();
            LOGGER.debug("HDFS Login via Spark Plugin Successful", Map.of("type", type, "principal", principal, "keytab", keytab, "krb5", krb5));
        } catch (Exception e) {
            LOGGER.error("Error in performing HDFS Login", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
            throw e;
        }
    }

    @Override
    public ExecutorPlugin executorPlugin() {
        return new ExecutorPlugin() {

            @SneakyThrows
            @Override
            public void init(final PluginContext ctx, final Map<String, String> conf) {
                LOGGER.debug("Invoking executorPlugin", Map.of("type", TYPE.executor));
//                startPrometheusServerThread(new AtomicReference<>()); // for future reference
                startResourceManager();
                PipelineUtils.workerId(SparkEnv.get().executorId());
                SparkUtils.setSparkRuntimeConf(ctx.conf());
                logPluginContext(TYPE.executor, ctx);
                Map<String, String> props = new HashMap<>();
                Arrays.stream(ctx.conf().getAll()).filter(x -> {
                    String key = String.valueOf(x._1);
                    return key.startsWith(SPARK_PROP_PREFIX) || key.startsWith(STARGATE_SPARK_PROP_PREFIX) || key.startsWith(STARGATE_SPARK_EXECUTOR_SYS_PREFIX);
                }).forEach(x -> props.put(String.valueOf(x._1), String.valueOf(x._2)));
                props.keySet().stream().filter(x -> x.startsWith(STARGATE_SPARK_EXECUTOR_SYS_PREFIX)).forEach(x -> System.setProperty(x.substring(STARGATE_SPARK_EXECUTOR_SYS_PREFIX.length()), props.get(x)));
                LOGGER.debug("Properties exposed to Executor", props.keySet());
                if (ctx.conf().getBoolean(STARGATE_SPARK_PROP_SHARED_FILES_COPY, true)) {
                    copySharedFiles(TYPE.executor, props, null, ctx);
                }
                performKerberosLogin(TYPE.executor, props, ctx);
            }
        };
    }

    private enum TYPE {
        driver, executor
    }

    public static final class RpcSharedFileRequest implements Serializable {
        private static final long serialVersionUID = 1L;
        private String fileName;

        public static RpcSharedFileRequest sharedFileRequest(final String fileName) {
            RpcSharedFileRequest obj = new RpcSharedFileRequest();
            obj.fileName = fileName;
            return obj;
        }
    }

    public static final class RpcSharedFileResponse implements Serializable {
        private static final long serialVersionUID = 1L;
        private String fileName;
        private byte[] content;

        public static RpcSharedFileResponse sharedFileResponse(final String fileName, final byte[] content) {
            RpcSharedFileResponse obj = new RpcSharedFileResponse();
            obj.fileName = fileName;
            obj.content = content;
            return obj;
        }
    }
}