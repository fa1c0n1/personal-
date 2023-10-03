package com.apple.aml.stargate.common.services;

import org.jdbi.v3.core.Handle;

import java.util.function.Consumer;

public interface JdbiService extends JdbcService {
    void invoke(final Consumer<Handle> consumer) throws Exception;
}
