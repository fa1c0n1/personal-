package com.apple.aml.stargate.common.options;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class CustomFieldOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    private String name;
    private List<String> fields;
}
