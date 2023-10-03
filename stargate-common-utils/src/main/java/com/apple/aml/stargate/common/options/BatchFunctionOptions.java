package com.apple.aml.stargate.common.options;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

@Data
@EqualsAndHashCode(callSuper = true)
public class BatchFunctionOptions extends BatchLambdaOptions implements Serializable {
    private String className;
}
