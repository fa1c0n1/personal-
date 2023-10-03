package com.apple.aml.stargate.cache.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class CacheEntry implements Serializable {
    private long expiry; // expiry time in milli seconds
    private Object payload;
}
