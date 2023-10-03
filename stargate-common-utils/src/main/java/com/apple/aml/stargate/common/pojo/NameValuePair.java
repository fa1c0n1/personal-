package com.apple.aml.stargate.common.pojo;

import lombok.Data;

import java.io.Serializable;

@Data
public class NameValuePair implements Serializable, Comparable {
    private static final long serialVersionUID = 1L;
    private String name;
    private String value;

    @Override
    public int compareTo(final Object o) {
        try {
            return this.name.compareTo(((NameValuePair) o).name);
        } catch (Exception e) {
            return 0;
        }
    }

    public Object value() {
        return KeyValuePair._value(this.value);
    }
}
