package com.apple.aml.stargate.pipeline.inject;

import com.apple.aml.stargate.common.options.JdbiOptions;
import com.apple.aml.stargate.common.services.JdbiService;
import com.apple.aml.stargate.common.utils.ContextHandler;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.argument.Arguments;
import org.jdbi.v3.core.argument.NullArgument;
import org.jdbi.v3.core.mapper.CaseStrategy;
import org.jdbi.v3.core.mapper.ColumnMappers;
import org.jdbi.v3.core.mapper.MapMappers;
import org.jdbi.v3.core.mapper.reflect.ReflectionMappers;
import org.jdbi.v3.core.result.ResultProducers;
import org.jdbi.v3.core.statement.SqlStatements;
import org.jdbi.v3.core.statement.StatementExceptions;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.SQL_EXECUTE_ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.SQL_EXECUTE_SUCCESS;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.SQL_INVOKE_ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.SQL_INVOKE_SUCCESS;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.SQL_SELECT_ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.SQL_SELECT_SUCCESS;
import static com.apple.aml.stargate.common.utils.ClassUtils.parseBoolean;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static java.lang.Integer.parseInt;

public class JdbiServiceHandler implements JdbiService, Serializable {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final ConcurrentHashMap<String, JdbiServiceHandler> HANDLER_CACHE = new ConcurrentHashMap<>();
    protected final String nodeName;
    protected final JdbiOptions options;
    protected transient HikariDataSource dataSource;
    private transient Jdbi jdbi;

    public static JdbiServiceHandler jdbiService(final String nodeName, final JdbiOptions options) {
        return HANDLER_CACHE.computeIfAbsent(nodeName, n -> new JdbiServiceHandler(nodeName, options));
    }

    public JdbiServiceHandler(final String nodeName, final JdbiOptions options) {
        this.nodeName = nodeName;
        this.options = options;
    }

    @Override
    public Connection getConnection() throws Exception {
        try {
            return dataSource().getConnection();
        } catch (Exception e) {
            LOGGER.error("Could not fetch jdbc connection", Map.of(NODE_NAME, nodeName, ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
            throw e;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <O> List<O> select(final Class<O> returnType, final String sql, Object... args) throws Exception {
        final Class type = returnType == null ? Map.class : returnType;
        long startTimeNanos = System.nanoTime();
        ContextHandler.Context ctx = ContextHandler.ctx();
        try (Handle handle = jdbi().open()) {
            List list = handle.select(sql, args).mapTo(type).list();
            histogramDuration(ctx.getNodeName(), ctx.getNodeType(), ctx.getSchema().getFullName(), SQL_SELECT_SUCCESS).observe((System.nanoTime() - startTimeNanos) / 1000000.0);
            return list;
        } catch (Exception e) {
            histogramDuration(ctx.getNodeName(), ctx.getNodeType(), ctx.getSchema().getFullName(), SQL_SELECT_ERROR).observe((System.nanoTime() - startTimeNanos) / 1000000.0);
            throw e;
        }
    }

    @Override
    public int execute(final String sql, final Object... args) throws Exception {
        long startTimeNanos = System.nanoTime();
        ContextHandler.Context ctx = ContextHandler.ctx();
        try (Handle handle = jdbi().open()) {
            int status = handle.execute(sql, args);
            histogramDuration(ctx.getNodeName(), ctx.getNodeType(), ctx.getSchema().getFullName(), SQL_EXECUTE_SUCCESS).observe((System.nanoTime() - startTimeNanos) / 1000000.0);
            return status;
        } catch (Exception e) {
            histogramDuration(ctx.getNodeName(), ctx.getNodeType(), ctx.getSchema().getFullName(), SQL_EXECUTE_ERROR).observe((System.nanoTime() - startTimeNanos) / 1000000.0);
            throw e;
        }
    }

    @Override
    public void invoke(final Consumer<Handle> consumer) throws Exception {
        long startTimeNanos = System.nanoTime();
        ContextHandler.Context ctx = ContextHandler.ctx();
        try (Handle handle = jdbi().open()) {
            consumer.accept(handle);
            histogramDuration(ctx.getNodeName(), ctx.getNodeType(), ctx.getSchema().getFullName(), SQL_INVOKE_SUCCESS).observe((System.nanoTime() - startTimeNanos) / 1000000.0);
        } catch (Exception e) {
            histogramDuration(ctx.getNodeName(), ctx.getNodeType(), ctx.getSchema().getFullName(), SQL_INVOKE_ERROR).observe((System.nanoTime() - startTimeNanos) / 1000000.0);
            throw e;
        }
    }

    protected HikariConfig hikariConfig() throws Exception {
        HikariConfig config = new HikariConfig();
        String driverName = options.driverName();
        if (!isBlank(driverName)) config.setDriverClassName(driverName);
        if (!isBlank(options.getConnectionString())) {
            config.setJdbcUrl(options.getConnectionString());
            config.addDataSourceProperty("url", options.getConnectionString());
        }
        String userName = options.userName();
        if (!isBlank(userName)) {
            config.setUsername(userName);
            config.addDataSourceProperty("user", userName);
            config.addDataSourceProperty("username", userName);
        }
        if (!isBlank(options.getPassword())) {
            config.setPassword(options.getPassword());
            config.addDataSourceProperty("password", options.getPassword());
        }
        if (!isBlank(options.getDatabase())) config.addDataSourceProperty("db", options.getDatabase());
        if (!isBlank(options.getSchema())) config.addDataSourceProperty("schema", options.getSchema());
        if (!isBlank(options.getRole())) config.addDataSourceProperty("role", options.getRole());
        if (options.getDatasourceProperties() != null) {
            options.getDatasourceProperties().forEach((k, v) -> {
                config.addDataSourceProperty(k, v);
            });
        }
        return config;
    }

    protected HikariDataSource dataSource() throws Exception {
        if (dataSource != null) return dataSource;
        dataSource = new HikariDataSource(hikariConfig());
        return dataSource;
    }

    protected Jdbi jdbi() throws Exception {
        if (jdbi != null) return jdbi;
        Jdbi jdbi = Jdbi.create(dataSource());
        jdbi.configure(Arguments.class, x -> {
            if (!isBlank(options.getBindingNullToPrimitivesPermitted())) x.setBindingNullToPrimitivesPermitted(parseBoolean(options.getBindingNullToPrimitivesPermitted()));
            if (!isBlank(options.getPreparedArgumentsEnabled())) x.setPreparedArgumentsEnabled(parseBoolean(options.getPreparedArgumentsEnabled()));
            if (!isBlank(options.getUntypedNullArgument())) x.setUntypedNullArgument(new NullArgument(parseInt(options.getUntypedNullArgument())));
        });
        jdbi.configure(ColumnMappers.class, x -> {
            if (!isBlank(options.getCoalesceNullPrimitivesToDefaults())) x.setCoalesceNullPrimitivesToDefaults(parseBoolean(options.getCoalesceNullPrimitivesToDefaults()));
        });
        jdbi.configure(MapMappers.class, x -> {
            if (!isBlank(options.getCaseChange())) x.setCaseChange(CaseStrategy.valueOf(options.getCaseChange().trim().toUpperCase()));
        });
        jdbi.configure(ReflectionMappers.class, x -> {
            if (!isBlank(options.getCaseChange())) x.setCaseChange(CaseStrategy.valueOf(options.getCaseChange().trim().toUpperCase()));
        });
        jdbi.configure(ResultProducers.class, x -> {
            if (!isBlank(options.getAllowNoResults())) x.allowNoResults(parseBoolean(options.getAllowNoResults()));
        });
        jdbi.configure(SqlStatements.class, x -> {
            if (options.getQueryTimeout() != null) x.setQueryTimeout((int) options.getQueryTimeout().getSeconds());
        });
        jdbi.configure(StatementExceptions.class, x -> {
            if (options.getExceptionLengthLimit() > 0) x.setLengthLimit(options.getExceptionLengthLimit());
            if (!isBlank(options.getExceptionMessageRendering())) x.setMessageRendering(StatementExceptions.MessageRendering.valueOf(options.getExceptionMessageRendering().trim().toUpperCase()));
        });
        this.jdbi = jdbi;
        return this.jdbi;
    }
}
