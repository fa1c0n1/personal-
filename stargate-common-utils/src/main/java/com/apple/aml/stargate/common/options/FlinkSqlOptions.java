package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;

import java.io.Serializable;

@Data
public class FlinkSqlOptions implements Serializable {

    private String sql;

    @Optional
    private String schemaId;

    @Optional
    private String preSqlSchemaId;

    @Optional
    private String fileBasePath;
    @Optional
    private String tableName;
    @Optional
    private String id;


}
