package com.apple.aml.stargate.common.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static com.apple.aml.stargate.common.utils.JsonUtils.readJson;

@Data
@AllArgsConstructor
@NoArgsConstructor
public final class KeyValuePair implements Serializable, Comparable {
    private static final long serialVersionUID = 1L;
    private String key;
    private String value;

    public static KeyValuePair pair(final String key, final String value) {
        return new KeyValuePair(key, value);
    }

    @Override
    public int compareTo(final Object o) {
        try {
            return this.key.compareTo(((KeyValuePair) o).key);
        } catch (Exception e) {
            return 0;
        }
    }

    public Object value() {
        return KeyValuePair._value(this.value);
    }

    public static Object _value(final String value) {
        if (value == null) {
            return null;
        }
        try {
            if (value.indexOf('{') == 0) {
                Map returnValue = readJson(value, Map.class);
                if (returnValue != null) {
                    return returnValue;
                }
                return value;
            }
            if (value.indexOf('[') == 0) {
                List returnValue = readJson(value, List.class);
                if (returnValue != null) {
                    return returnValue;
                }
                return value;
            }
        } catch (Exception e) {
            return value;
        }
        return value;
    }
}
