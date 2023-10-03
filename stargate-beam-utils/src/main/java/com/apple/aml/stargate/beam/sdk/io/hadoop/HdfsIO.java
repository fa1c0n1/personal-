package com.apple.aml.stargate.beam.sdk.io.hadoop;

import com.apple.aml.stargate.beam.sdk.io.file.GenericFileIO;
import com.apple.aml.stargate.beam.sdk.options.StargateOptions;
import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.HdfsOptions;
import com.apple.aml.stargate.pipeline.sdk.io.hadoop.HdfsSharedConfiguration;
import com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.common.collect.ImmutableList;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.beam.sdk.utils.FileWriterFns.codecName;
import static com.apple.aml.stargate.common.constants.CommonConstants.DEFAULT_DELIMITER;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_HDFS_SHARED_CONFIG_FILE;
import static com.apple.aml.stargate.common.constants.PipelineConstants.Krb5Properties.DEFAULT_FILENAME_KERBEROS_KEYTAB;
import static com.apple.aml.stargate.common.constants.PipelineConstants.Krb5Properties.DEFAULT_FILENAME_KERBEROS_KRB5;
import static com.apple.aml.stargate.common.constants.PipelineConstants.SparkProperties.SPARK_HADOOP_PROP_PREFIX;
import static com.apple.aml.stargate.common.constants.PipelineConstants.SparkProperties.STARGATE_SPARK_EXECUTOR_PROPERTIES_FILE;
import static com.apple.aml.stargate.common.constants.PipelineConstants.SparkProperties.STARGATE_SPARK_EXECUTOR_SYS_PREFIX;
import static com.apple.aml.stargate.common.constants.PipelineConstants.SparkProperties.STARGATE_SPARK_PROP_KERBEROS_KEYTAB;
import static com.apple.aml.stargate.common.constants.PipelineConstants.SparkProperties.STARGATE_SPARK_PROP_KERBEROS_KRB5;
import static com.apple.aml.stargate.common.constants.PipelineConstants.SparkProperties.STARGATE_SPARK_PROP_KERBEROS_PRINCIPAL;
import static com.apple.aml.stargate.common.constants.PipelineConstants.SparkProperties.STARGATE_SPARK_PROP_SHARED_FILES;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.io.hadoop.HdfsSharedConfiguration.SYS_PROP_KRB5_CONF;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.getKrb5FileContent;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static java.util.Collections.singleton;
import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;

public class HdfsIO extends GenericFileIO {
    public static final String HIVE_WAREHOUSE_KEY = "hive.metastore.warehouse.dir";
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final String DFS_NAMESERVICES_KEY = "dfs.nameservices";
    private static final String SPARK_HDFS_ACCESS_KEY = "spark.kerberos.access.hadoopFileSystems";

    @SuppressWarnings("unchecked")
    public static void init(final PipelineOptions pOptions, final StargateNode node) throws Exception {
        initWithDefaults(pOptions, node, PipelineConstants.NODE_TYPE.Hdfs.getConfig().getWriterDefaults());
    }

    @SuppressWarnings("unchecked")
    public static void initWithDefaults(final PipelineOptions pOptions, final StargateNode node, final Map<String, Object> defaults) throws Exception {
        LOGGER.debug("Setting up hdfs configs - started");
        HdfsOptions options = (HdfsOptions) node.getConfig();
        StargateOptions pipelineOptions = pOptions.as(StargateOptions.class);
        options.enableSchemaLevelDefaults();
        boolean dryRun = pipelineOptions.isDryRun();

        String sharedDir = pipelineOptions.getSharedDirectoryPath();
        Map<String, String> sparkProperties = new HashMap<>();
        Map<String, Object> coreProperties = new HashMap<>();
        Map<String, Object> hdfsProperties = new HashMap<>();
        Map<String, Object> hiveProperties = new HashMap<>();
        if (defaults != null && !defaults.isEmpty()) {
            for (Map.Entry<String, Object> entry : defaults.entrySet()) {
                if (entry.getValue() == null) {
                    continue;
                }
                String[] keySplit = entry.getKey().split("\\.");
                String value = entry.getValue().toString();
                String key = entry.getKey();
                Map map = null;
                if (keySplit.length > 1) {
                    String[] tokens = new String[keySplit.length - 1];
                    System.arraycopy(keySplit, 1, tokens, 0, keySplit.length - 1);
                    key = Arrays.stream(tokens).collect(Collectors.joining("."));
                    if ("core".equalsIgnoreCase(keySplit[0])) {
                        map = coreProperties;
                    } else if ("hdfs".equalsIgnoreCase(keySplit[0])) {
                        map = hdfsProperties;
                    } else if ("hive".equalsIgnoreCase(keySplit[0])) {
                        map = hiveProperties;
                    } else {
                        sparkProperties.put(entry.getKey(), value);
                    }
                }
                if (map != null) {
                    map.put(key, value);
                }
            }
        }
        StringBuilder coreBuilder = new StringBuilder();
        StringBuilder hdfsBuilder = new StringBuilder();
        StringBuilder hiveBuilder = new StringBuilder();
        loadConfigXmlProperties(node, options.getCoreXmlFileName(), options.getCoreProperties(), coreProperties);
        loadConfigXmlProperties(node, options.getHdfsXmlFileName(), options.getHdfsProperties(), hdfsProperties);
        loadConfigXmlProperties(node, options.getHiveXmlFileName(), options.getHiveProperties(), hiveProperties);

        Configuration config = PipelineUtils.getHadoopConfigReference().getLeft();
        Path propsFile = Paths.get(sharedDir, STARGATE_SPARK_EXECUTOR_PROPERTIES_FILE);
        setHadoopConfigFromExistingPropsFile(dryRun, config, propsFile);

        coreProperties.put(FS_DEFAULT_NAME_KEY, isBlank(options.getDefaultFsName()) ? String.format("hdfs://localhost") : options.getDefaultFsName());
        String nameservices = (String) hdfsProperties.get(DFS_NAMESERVICES_KEY);
        if (!isBlank(nameservices) && !hdfsProperties.containsKey("dfs.client.failover.proxy.provider." + nameservices)) {
            String key = "dfs.client.failover.proxy.provider." + nameservices;
            String value = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider";
            hdfsProperties.put(key, value);
        }

        boolean enableKerberos = !isBlank(options.getPrincipal()) && node.configFiles() != null && !isBlank(options.keytabFileName()) && node.configFiles().get(options.keytabFileName()) != null && !isBlank(options.krb5FileName());
        String keytabFileName = null;
        String krb5FileName = null;

        if (enableKerberos) {
            sparkProperties.put("com.sun.security.auth.module.Krb5LoginModule", "required");
            keytabFileName = String.format("%s/%s", sharedDir, DEFAULT_FILENAME_KERBEROS_KEYTAB);
            krb5FileName = String.format("%s/%s", sharedDir, DEFAULT_FILENAME_KERBEROS_KRB5);
            if (!dryRun) {
                Files.write(Paths.get(krb5FileName), getKrb5FileContent(node, options.getKrb5(), options.krb5FileName(), sharedDir, keytabFileName));
                Files.write(Paths.get(keytabFileName), (byte[]) node.configFiles().get(options.keytabFileName()));
            }
            boolean hadoopKrbEnabled = isBlank((String) coreProperties.get("hadoop.security.authentication")) || "kerberos".equalsIgnoreCase((String) coreProperties.get("hadoop.security.authentication"));
            if (hadoopKrbEnabled) {
                hdfsProperties.put("dfs.namenode.keytab.file", keytabFileName);
                hdfsProperties.put("dfs.datanode.keytab.file", keytabFileName);
                coreProperties.put("hadoop.security.authentication", "kerberos");
            }
            sparkProperties.put(STARGATE_SPARK_PROP_KERBEROS_KEYTAB, keytabFileName);
            sparkProperties.put(STARGATE_SPARK_PROP_KERBEROS_KRB5, krb5FileName);
            sparkProperties.put(STARGATE_SPARK_PROP_KERBEROS_PRINCIPAL, options.getPrincipal());
            sparkProperties.put("spark.kubernetes.kerberos.keytab", keytabFileName);
            sparkProperties.put("spark.kubernetes.kerberos.krb5.path", krb5FileName);
            sparkProperties.put("spark.kubernetes.kerberos.principal", options.getPrincipal());
            sparkProperties.put("spark.kerberos.principal", options.getPrincipal());
            sparkProperties.put("spark.kerberos.keytab", keytabFileName);
            sparkProperties.put("spark.kubernetes.kerberos.enabled", "true");
            hiveProperties.put("hive.server2.authentication", "kerberos");
        }
        if (options.getSparkProperties() != null && !options.getSparkProperties().isEmpty()) {
            for (Map.Entry<String, Object> entry : options.getSparkProperties().entrySet()) {
                sparkProperties.put("spark." + entry.getKey(), String.valueOf(entry.getValue()));
            }
        }
        if (!sparkProperties.containsKey(SPARK_HDFS_ACCESS_KEY)) {
            String hdfsRpcAddresses = hdfsProperties.keySet().stream().filter(x -> x.startsWith("dfs.namenode.rpc-address")).map(x -> hdfsProperties.get(x)).map(x -> x.toString().startsWith("hdfs://") ? x.toString() : "hdfs://" + x).collect(Collectors.joining(","));
            if (!isBlank(hdfsRpcAddresses)) {
                sparkProperties.put(SPARK_HDFS_ACCESS_KEY, hdfsRpcAddresses);
            }
        }

        applyProperties(dryRun, config, sharedDir, sparkProperties, coreBuilder, coreProperties, "core-site.xml");
        applyProperties(dryRun, config, sharedDir, sparkProperties, hdfsBuilder, hdfsProperties, "hdfs-site.xml");
        applyProperties(dryRun, config, sharedDir, sparkProperties, hiveBuilder, hiveProperties, "hive-site.xml");

        if (options.isAutoUpdateTable()) {
            if (isBlank(options.getDbPath()) && (hiveProperties == null || !hiveProperties.containsKey(HIVE_WAREHOUSE_KEY))) {
                options.setDbPath(options.basePath() + "/db");
            }
        }

        HdfsSharedConfiguration sharedConfiguration = new HdfsSharedConfiguration();
        if (enableKerberos) {
            sharedConfiguration.setPrincipal(options.getPrincipal());
            sharedConfiguration.setKeytabFileName(keytabFileName);
            sharedConfiguration.setKrb5FileName(krb5FileName);
        }
        sharedConfiguration.setCoreXmlFileName(sharedDir + "/core-site.xml");
        sharedConfiguration.setHdfsXmlFileName(sharedDir + "/hdfs-site.xml");
        sharedConfiguration.setHiveXmlFileName(sharedDir + "/hive-site.xml");
        sharedConfiguration.setConfig(new HashMap<>());
        for (Map.Entry<String, String> entry : sparkProperties.entrySet()) {
            String key = String.valueOf(entry.getKey());
            String value = String.valueOf(entry.getValue());
            if (!key.startsWith(SPARK_HADOOP_PROP_PREFIX)) {
                continue;
            }
            key = key.substring(SPARK_HADOOP_PROP_PREFIX.length());
            sharedConfiguration.getConfig().put(key, value);
        }
        if (!dryRun) Files.write(Paths.get(sharedDir, STARGATE_HDFS_SHARED_CONFIG_FILE), singleton(jsonString(sharedConfiguration)));

        Set<String> sharedFiles = new HashSet<>(Arrays.asList("core-site.xml", "hdfs-site.xml", "hive-site.xml", DEFAULT_FILENAME_KERBEROS_KEYTAB, DEFAULT_FILENAME_KERBEROS_KRB5, STARGATE_HDFS_SHARED_CONFIG_FILE));
        if (enableKerberos) {
            sharedFiles.add(DEFAULT_FILENAME_KERBEROS_KEYTAB);
            sharedFiles.add(DEFAULT_FILENAME_KERBEROS_KRB5);
        }
        sparkProperties.put(STARGATE_SPARK_PROP_SHARED_FILES, sharedFiles.stream().collect(Collectors.joining(DEFAULT_DELIMITER)));
        if (enableKerberos) {
            if (!dryRun) {
                System.setProperty(SYS_PROP_KRB5_CONF, krb5FileName);
                System.setProperty("security.kerberos.login.principal", options.getPrincipal());
                System.setProperty("security.kerberos.login.keytab", keytabFileName);
            }
            sparkProperties.put(STARGATE_SPARK_EXECUTOR_SYS_PREFIX + SYS_PROP_KRB5_CONF, keytabFileName);
        }
        config.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, false);
        try {
            Class supplierClass = Class.forName("org.apache.parquet.avro.GenericDataSupplier");
            AvroReadSupport.setAvroDataSupplier(config, supplierClass);
        } catch (Exception e) {
            LOGGER.debug("Could not set AvroDataSupplier. Will refer to defaults.", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage())));
        }
        CompressionCodecName codecName = codecName(options);
        if (codecName != null) {
            config.set("compression", codecName.name());
            config.set("parquet.compression", codecName.name());
        }
        if (!dryRun) {
            UserGroupInformation.setConfiguration(config);
            PipelineUtils.appendHadoopConfiguration(sparkProperties, sharedFiles.stream().filter(x -> x.endsWith(".xml")).map(x -> sharedDir + "/" + x).collect(Collectors.toList()));
            PipelineUtils.performKerberosLogin(false);
        }
        LOGGER.debug("Setting up hdfs configs - successful");
    }

    private static void loadConfigXmlProperties(final StargateNode node, final String suppliedFileName, final Map<String, Object> suppliedProps, final Map<String, Object> properties) throws Exception {
        if (suppliedProps != null && !suppliedProps.isEmpty()) {
            properties.putAll(suppliedProps);
        }
        if (suppliedFileName == null || suppliedFileName.isBlank() || !node.configFiles().containsKey(suppliedFileName)) {
            return;
        }
        String xml = new String(((byte[]) node.configFiles().get(suppliedFileName)));
        if (xml.isBlank()) {
            return;
        }
        XmlMapper mapper = new XmlMapper();
        HdfsConfiguration conf = mapper.readValue(xml, HdfsConfiguration.class);
        Properties props = conf.getAllProperties();
        if (props == null || props.isEmpty()) {
            return;
        }
        for (Map.Entry<Object, Object> entry : props.entrySet()) {
            properties.put(String.valueOf(entry.getKey()), entry.getValue());
        }
    }

    private static void setHadoopConfigFromExistingPropsFile(final boolean dryRun, final Configuration config, final Path propsFile) {
        if (!Files.exists(propsFile) || !Files.isRegularFile(propsFile)) {
            return;
        }
        Properties props = new Properties();
        try (InputStream stream = new FileInputStream(propsFile.toFile().getAbsolutePath())) {
            props.load(stream);
        } catch (Exception e) {
            LOGGER.error("Could not load additional system properties", e);
        }
        for (Map.Entry<Object, Object> entry : props.entrySet()) {
            String key = String.valueOf(entry.getKey());
            String value = String.valueOf(entry.getValue());
            if (!dryRun) System.setProperty(key, value);
            if (key.startsWith(SPARK_HADOOP_PROP_PREFIX)) {
                key = key.substring(SPARK_HADOOP_PROP_PREFIX.length());
            }
            config.set(key, value);
        }
    }

    private static void applyProperties(final boolean dryRun, final Configuration hconf, final String sharedDir, final Map<String, String> sparkProperties, final StringBuilder builder, final Map<String, Object> props, final String fileName) throws IOException {
        for (Map.Entry<String, Object> entry : props.entrySet()) {
            applyConfigProperty(sparkProperties, builder, hconf, entry);
        }
        if (!dryRun) Files.write(Paths.get(sharedDir, fileName), singleton("<configuration>\n" + builder + "</configuration>"));
    }

    private static void applyConfigProperty(final Map<String, String> properties, final StringBuilder builder, final Configuration conf, final Map.Entry<String, Object> entry) {
        if (entry.getValue() == null) {
            return;
        }
        String key = entry.getKey();
        String value = entry.getValue().toString();
        conf.set(key, value);
        builder.append("<property><name>").append(key).append("</name><value>").append(value).append("</value></property>\n");
        properties.put(SPARK_HADOOP_PROP_PREFIX + key, value);
    }

    public void initRead(final Pipeline pipeline, final StargateNode node) throws Exception {
        initCommon(pipeline, node);
    }

    @SuppressWarnings("unchecked")
    public void initCommon(final Pipeline pipeline, final StargateNode node) throws Exception {
        HadoopFileSystemOptions fsOptions = pipeline.getOptions().as(HadoopFileSystemOptions.class);
        Configuration config = PipelineUtils.getHadoopConfigReference().getLeft();
        fsOptions.setHdfsConfiguration(ImmutableList.of(config));
        FileSystems.setDefaultPipelineOptions(pipeline.getOptions());
    }

    public SCollection<KV<String, GenericRecord>> write(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        return super.write(pipeline, node, collection);
    }

    public void initWrite(final Pipeline pipeline, final StargateNode node) throws Exception {
        initCommon(pipeline, node);
    }

    public void initTransform(final Pipeline pipeline, final StargateNode node) throws Exception {
        initCommon(pipeline, node);
    }

    public SCollection<KV<String, GenericRecord>> transform(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        return super.transformCollection(pipeline, node, collection, true);
    }
}
