package com.apple.aml.stargate.common.options;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

@Data
@EqualsAndHashCode(callSuper = true)
public class ExternalBatchFunctionOptions extends ExternalFunctionOptions implements Serializable {
    private static final long serialVersionUID = 1L;
}
