package com.apple.aml.stargate.common.pojo;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class Holder<O> implements Serializable {
    private static final long serialVersionUID = 1L;
    protected O value;
}
