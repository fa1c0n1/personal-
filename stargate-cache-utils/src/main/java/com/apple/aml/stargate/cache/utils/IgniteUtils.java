package com.apple.aml.stargate.cache.utils;

import com.apple.aml.stargate.common.utils.AppConfig;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.typesafe.config.Config;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.kubernetes.configuration.KubernetesConnectionConfiguration;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.kubernetes.TcpDiscoveryKubernetesIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;
import org.apache.ignite.spi.metric.opencensus.OpenCensusMetricExporterSpi;
import org.slf4j.Logger;
import org.springframework.beans.BeanUtils;

import java.beans.PropertyDescriptor;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.OfsConstants.APP_IGNITE_DISCOVERY_TYPE;
import static com.apple.aml.stargate.common.utils.AppConfig.env;
import static com.apple.aml.stargate.common.utils.JsonUtils.fastJsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJson;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static com.typesafe.config.ConfigRenderOptions.concise;
import static org.apache.ignite.configuration.CacheConfiguration.DFLT_BACKUPS;

public final class IgniteUtils {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    private IgniteUtils() {
    }

    @SuppressWarnings("unchecked")
    public static IgniteConfiguration igniteConfiguration() throws Exception {
        Config config = AppConfig.config();
        IgniteConfiguration cfg = new IgniteConfiguration();
        for (PropertyDescriptor descriptor : BeanUtils.getPropertyDescriptors(IgniteConfiguration.class)) {
            String paramName = descriptor.getName();
            if (!config.hasPath("ignite." + paramName)) {
                continue;
            }
            Method method = descriptor.getWriteMethod();
            if (method == null) {
                continue;
            }
            if (method.getParameterCount() != 1) {
                continue;
            }
            Class paramClass = method.getParameterTypes()[0];
            if (!(paramClass.isPrimitive() || paramClass.equals(String.class))) {
                continue;
            }
            method.invoke(cfg, config.getAnyRef("ignite." + paramName));
        }
        if (isBlank(cfg.getWorkDirectory())) {
            cfg.setWorkDirectory(String.format("%s/.ignite", System.getProperty("user.home")));
        }
        cfg.setGridLogger(new Slf4jLogger());
        OpenCensusMetricExporterSpi metricsSpi = new OpenCensusMetricExporterSpi();
        metricsSpi.setSendConsistentId(true);
        metricsSpi.setSendConsistentId(true);
        metricsSpi.setSendNodeId(true);
        metricsSpi.setPeriod(config.getLong("ignite.metricExporterSpiPeriod"));
        cfg.setMetricExporterSpi(metricsSpi, new JmxMetricExporterSpi());
        cfg.setFailureHandler(new IgniteFailureHandler());
        int availableProcessorsCount = Runtime.getRuntime().availableProcessors();
        cfg.setSystemThreadPoolSize(Integer.max(availableProcessorsCount, config.getInt("ignite.systemThreadPoolSize")));
        cfg.setQueryThreadPoolSize(Integer.max(availableProcessorsCount, config.getInt("ignite.queryThreadPoolSize")));
        TcpCommunicationSpi commSpi = readJson(config.getConfig("ignite.communicationSpi").root().render(concise()), TcpCommunicationSpi.class);
        cfg.setCommunicationSpi(commSpi);
        SqlConfiguration sqlConfiguration = readJson(config.getConfig("ignite.sqlConfiguration").root().render(concise()), SqlConfiguration.class);
        cfg.setSqlConfiguration(sqlConfiguration);
        TcpDiscoverySpi discoverySpi = readJson(config.getConfig("ignite.discoverySpi").root().render(concise()), TcpDiscoverySpi.class);
        String discoveryType = env(APP_IGNITE_DISCOVERY_TYPE, config.hasPath("ignite.discoverySpi.discoveryType") ? config.getString("ignite.discoverySpi.discoveryType") : null);
        if (discoveryType == null || "k8s".equalsIgnoreCase(discoveryType)) {
            KubernetesConnectionConfiguration k8sConfig = readJson(config.getConfig("ignite.discoverySpi.discovery").root().render(concise()), KubernetesConnectionConfiguration.class);
            String k8sApiServerUrl = null;
            if (config.hasPath("ignite.discoverySpi.discovery.k8sApiServerUrl")) {
                k8sApiServerUrl = config.getString("ignite.discoverySpi.discovery.k8sApiServerUrl");
                if (isInvalidUrl(k8sApiServerUrl)) {
                    k8sApiServerUrl = null;
                }
            }
            if (k8sApiServerUrl == null) {
                String k8sHost = env("KUBERNETES_SERVICE_HOST", null);
                String k8sPort = env("KUBERNETES_SERVICE_PORT_HTTPS", null);
                if (!(k8sHost == null || k8sHost.trim().isBlank() || k8sPort == null || k8sPort.trim().isBlank() || Integer.parseInt(k8sPort) <= 0)) {
                    k8sApiServerUrl = String.format("https://%s:%s", k8sHost.trim(), k8sPort.trim());
                    if (isInvalidUrl(k8sApiServerUrl)) {
                        k8sApiServerUrl = null;
                    }
                }
            }
            if (k8sApiServerUrl != null) {
                k8sConfig.setMasterUrl(k8sApiServerUrl);
            }
            LOGGER.debug("KubernetesConnectionConfiguration is set to", Map.of("k8sConfig", k8sConfig, "k8sApiServerUrl", k8sApiServerUrl));
            TcpDiscoveryKubernetesIpFinder ipFinder = new TcpDiscoveryKubernetesIpFinder(k8sConfig);
            discoverySpi.setIpFinder(ipFinder);
        } else if (discoveryType.toLowerCase().startsWith("multi")) {
            TcpDiscoveryMulticastIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();
            ipFinder.setMulticastGroup(config.getString("ignite.discoverySpi.discovery.multicastGroup"));
            discoverySpi.setIpFinder(ipFinder);
        }
        LOGGER.debug("TcpDiscoverySpi config is set to", Map.of("discoverySpi", discoverySpi));
        cfg.setDiscoverySpi(discoverySpi);
        cfg.setDataStorageConfiguration(parseDataStorageConfig(config));
        Config cacheConfigs = config.getConfig("ignite.cacheDetails");
        List<CacheConfiguration> configs = new ArrayList<>();
        for (final String cacheName : cacheConfigs.root().keySet()) {
            Config ccConfig = cacheConfigs.getConfig(cacheName);
            CacheConfiguration cacheConfiguration = parseCacheConfig(cacheName, ccConfig, cfg);
            configs.add(cacheConfiguration);
        }
        cfg.setCacheConfiguration(configs.toArray(new CacheConfiguration[configs.size()]));
        if (config.hasPath("ignite.clientConnectorConfiguration")) {
            ClientConnectorConfiguration clientConnectorConfiguration = readJson(config.getConfig("ignite.clientConnectorConfiguration").root().render(concise()), ClientConnectorConfiguration.class);
            cfg.setClientConnectorConfiguration(clientConnectorConfiguration);
        }
        return cfg;
    }

    private static boolean isInvalidUrl(final String urlPath) {
        try {
            URI url = new URI(urlPath);
            return new InetSocketAddress(url.getHost(), url.getPort()).isUnresolved();
        } catch (Exception e) {
            return true;
        }
    }

    private static DataStorageConfiguration parseDataStorageConfig(Config config) throws JsonProcessingException {
        DataStorageConfiguration dataStorageConfiguration = readJson(config.getConfig("ignite.dataStorageConfiguration").root().render(concise()), DataStorageConfiguration.class);
        Config dataStorageRegionConfigs = config.getConfig("ignite.dataStorageRegions");
        LinkedList<DataRegionConfiguration> dataRegionConfigurations = new LinkedList<>();
        for (final String dataStorageRegionName : dataStorageRegionConfigs.root().keySet()) {
            DataRegionConfiguration dataRegionConfiguration = readJson(dataStorageRegionConfigs.getConfig(dataStorageRegionName).root().render(concise()), DataRegionConfiguration.class);
            if ("default".equalsIgnoreCase(dataStorageRegionName)) {
                dataStorageConfiguration.setDefaultDataRegionConfiguration(dataRegionConfiguration);
            } else {
                dataRegionConfigurations.add(dataRegionConfiguration);
            }
        }
        dataStorageConfiguration.setDataRegionConfigurations(dataRegionConfigurations.toArray(new DataRegionConfiguration[dataRegionConfigurations.size()]));
        return dataStorageConfiguration;
    }

    @SuppressWarnings("unchecked")
    private static CacheConfiguration parseCacheConfig(final String cacheName, final Config ccConfig, final IgniteConfiguration igniteConfiguration) throws JsonProcessingException {
        CacheConfiguration cacheConfiguration = readJson(ccConfig.root().render(concise()), CacheConfiguration.class);
        if (cacheConfiguration.getName() == null) {
            cacheConfiguration.setName(cacheName);
        }
        if (ccConfig.hasPath("maxEntries") && ccConfig.getInt("maxEntries") > 0) {
            cacheConfiguration.setEvictionPolicyFactory(() -> new LruEvictionPolicy(ccConfig.getInt("maxEntries")));
        }
        if (ccConfig.hasPath("backups") && ccConfig.getInt("backups") > 0) {
            cacheConfiguration.setBackups(ccConfig.getInt("backups"));
        } else {
            cacheConfiguration.setBackups(DFLT_BACKUPS);
        }
        // we'll just use default region for now
        //if (Arrays.stream(igniteConfiguration.getDataStorageConfiguration().getDataRegionConfigurations()).anyMatch(r -> cacheName.equals(r.getName()))) {
        //    cacheConfiguration.setDataRegionName(cacheName);
        //}
        return cacheConfiguration;
    }

    @SuppressWarnings("unchecked")
    public static Ignite initIgnite(final IgniteConfiguration configuration) throws Exception {
        Config config = AppConfig.config();
        String configString;
        try {
            configString = fastJsonString(configuration);
        } catch (Exception e) {
            configString = configuration.toString();
        }
        LOGGER.info("Initializing ignite using configuration", Map.of("configuration", configString));
        Ignite ignite;
        boolean localStart = false;
        try {
            ignite = Ignition.ignite(configuration.getIgniteInstanceName());
        } catch (Exception e) {
            LOGGER.info("Could not fetch ignite instance. Assuming instance not available yet and will try to start the server", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage())));
            ignite = Ignition.start(configuration);
            ignite.cluster().state(ClusterState.ACTIVE);
            localStart = true;
        }
        IgniteCluster cluster = ignite.cluster();
        cluster.baselineAutoAdjustEnabled(true);
        cluster.baselineAutoAdjustTimeout(config.getLong("ignite.baselineAutoAdjustTimeout"));
        IgniteProductVersion version = ignite.version();
        Collection<String> cacheNames = ignite.cacheNames();
        Set<String> cacheConfigNames = Arrays.stream(configuration.getCacheConfiguration()).map(c -> c.getName()).collect(Collectors.toSet());
        LOGGER.info("Ignite init successfully", Map.of("clusterId", cluster.id().toString(), "version", version.toString(), "state", cluster.state().name(), "localStart", localStart, "cacheConfigNames", cacheConfigNames, "cacheNames", cacheNames));
        Config cacheTemplateConfigs = config.getConfig("ignite.cacheTemplates");
        for (final String templateName : cacheTemplateConfigs.root().keySet()) {
            Config ccConfig = cacheTemplateConfigs.getConfig(templateName);
            CacheConfiguration cacheConfiguration = parseCacheConfig(templateName, ccConfig, configuration);
            ignite.addCacheConfiguration(cacheConfiguration);
            LOGGER.info("Added new cache config template", Map.of("templateName", templateName));
        }
        Set<String> hostNames = new HashSet<>();
        Set<String> addresses = new HashSet<>();
        Map<String, Map<String, Object>> info = new HashMap<>();
        cluster.topology(cluster.topologyVersion()).forEach(node -> {
            info.put(node.id().toString(), Map.of("addresses", node.addresses(), "order", node.order(), "hostNames", node.hostNames()));
            hostNames.addAll(node.hostNames());
            addresses.addAll(node.addresses());
        });
        addresses.remove("127.0.0.1");
        LOGGER.info("Current cluster topology", info, Map.of("hostNames", hostNames, "addresses", addresses, "clusterId", cluster.id().toString(), "clusterTag", cluster.tag()));
        return ignite;
    }

    public static List<Map> executeIgniteCacheQuery(final Ignite ignite, final String cacheName, final String sql, final boolean localOnly) {
        return executeIgniteCacheQuery(ignite.cache(cacheName), sql, localOnly);
    }

    @SuppressWarnings({"unchecked", "deprecation"})
    public static List<Map> executeIgniteCacheQuery(final IgniteCache igniteCache, final String sql, final boolean localOnly) {
        SqlFieldsQuery sqlFieldsQuery = new SqlFieldsQuery(sql);
        if (localOnly) {
            sqlFieldsQuery = sqlFieldsQuery.setLocal(true).setCollocated(true).setReplicatedOnly(true);
        }
        List<Map> returnList = new ArrayList<>();
        try (FieldsQueryCursor<List<?>> cursor = igniteCache.query(sqlFieldsQuery)) {
            cursor.iterator().forEachRemaining(row -> {
                int outputColumnCount = 0;
                try {
                    outputColumnCount = cursor.getColumnsCount();
                } catch (Exception ignored) {

                }
                if (outputColumnCount <= 0) {
                    return;
                }
                Map<String, Object> map = new HashMap<>();
                for (int i = outputColumnCount - 1; i >= 0; i--) {
                    map.put(cursor.getFieldName(i), row.get(i));
                }
                returnList.add(map);
            });
        }
        return returnList;
    }

    public static boolean executeIgniteCacheDML(final Ignite ignite, final String cacheName, final String sql, final boolean localOnly) {
        return executeIgniteCacheDML(ignite.cache(cacheName), sql, localOnly);
    }

    @SuppressWarnings({"unchecked", "deprecation"})
    public static boolean executeIgniteCacheDML(final IgniteCache igniteCache, final String sql, final boolean localOnly) {
        SqlFieldsQuery sqlFieldsQuery = new SqlFieldsQuery(sql);
        igniteCache.query(sqlFieldsQuery).getAll();
        return true;
    }
}