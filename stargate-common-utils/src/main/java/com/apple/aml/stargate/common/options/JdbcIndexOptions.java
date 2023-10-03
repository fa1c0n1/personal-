package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.List;

@Data
@EqualsAndHashCode
public class JdbcIndexOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    @EqualsAndHashCode.Exclude
    private String name;
    private List<String> columns;
    @Optional
    private boolean unique;
}
