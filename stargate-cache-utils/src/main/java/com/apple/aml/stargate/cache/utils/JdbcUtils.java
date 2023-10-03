package com.apple.aml.stargate.cache.utils;

import org.slf4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;

import java.lang.invoke.MethodHandles;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.apple.aml.stargate.common.utils.LogUtils.logger;

public final class JdbcUtils {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    private JdbcUtils() {
    }

    @SuppressWarnings("unchecked")
    public static List<Map> getSQLResults(final JdbcTemplate jdbcTemplate, final String query) {
        List<Map<String, Object>> results = jdbcTemplate.queryForList(query);
        return (List<Map>) (Object) results;
    }

    public static List<Map> fetchSQLResults(final JdbcTemplate jdbcTemplate, final String query) {
        List<Map> results = jdbcTemplate.query(query, (rs, rowNum) -> {
            Map<String, Object> record = new HashMap<>();
            ResultSetMetaData metaData = rs.getMetaData();
            for (int i = metaData.getColumnCount(); i > 0; i--) {
                record.put(metaData.getColumnName(i).toUpperCase(), rs.getObject(i));
            }
            return record;
        });
        return results;
    }

    public static Boolean executeDML(final JdbcTemplate jdbcTemplate, final String query) {
        jdbcTemplate.execute(query);
        return true;
    }
}
