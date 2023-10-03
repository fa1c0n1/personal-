package com.apple.aml.stargate.cache.ofs;

import com.apple.aml.nativeml.spec.OFSService;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Lazy;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Map;

import static com.apple.aml.stargate.cache.utils.CacheMetrics.READ_LATENCIES_HISTOGRAM;
import static com.apple.aml.stargate.cache.utils.JdbcUtils.fetchSQLResults;
import static com.apple.aml.stargate.common.constants.CommonConstants.MDCKeys.SOURCE_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.CACHE_PROVIDER_JDBC;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.CACHE_TYPE_RELATIONAL;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.EXCEPTION;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.OK;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.OVERALL;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SUCCESS;
import static com.apple.aml.stargate.common.utils.LogUtils.getFromMDCOrDefault;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;

@Service
@ConditionalOnProperty(prefix = "stargate.cache.ofs", name = "type", havingValue = "ignite-jdbc")
@Lazy
public class IgniteJdbcOFSService extends IgniteLocalOFSService implements OFSService {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    @Qualifier("ignite-jdbc-template")
    @Autowired
    private JdbcTemplate jdbcTemplate;

    @PostConstruct
    public void init() {
        super.init();
    }

    public Map getRecord(final String query) {
        long startTime = System.nanoTime();
        Map result = null;
        try {
            Collection<Map> returnList = fetchSQLResults(jdbcTemplate, query);
            if (returnList != null && !returnList.isEmpty()) {
                result = returnList.iterator().next();
            }
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_JDBC, CACHE_TYPE_RELATIONAL, "getRecord", OVERALL, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
        } catch (Exception e) {
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_JDBC, CACHE_TYPE_RELATIONAL, "getRecord", OVERALL, ERROR, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
            LOGGER.error("Error querying cache", Map.of("query", query), e);
        }
        return result;
    }

    public Collection<Map> getRecords(final String query) {
        long startTime = System.nanoTime();
        Collection<Map> result = null;
        try {
            result = fetchSQLResults(jdbcTemplate, query);
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_JDBC, CACHE_TYPE_RELATIONAL, "getRecords", OVERALL, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
        } catch (Exception e) {
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_JDBC, CACHE_TYPE_RELATIONAL, "getRecords", OVERALL, ERROR, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
            LOGGER.error("Error querying from ignite", Map.of("query", query), e);
        }
        return result;
    }
}
