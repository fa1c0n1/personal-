package com.apple.aml.stargate.common.services;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

public interface JdbcService {
    Connection getConnection() throws Exception;

    <O> List<O> select(final Class<O> returnType, final String sql, Object... args) throws Exception;

    default List<Map> select(final String sql, Object... args) throws Exception {
        return select(Map.class, sql, args);
    }

    int execute(final String sql, Object... args) throws Exception;
}
