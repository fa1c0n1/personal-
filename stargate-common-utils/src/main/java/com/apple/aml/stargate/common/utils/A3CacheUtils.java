package com.apple.aml.stargate.common.utils;

import com.apple.ist.idms.i3.a3client.pub.A3TokenInfo;
import org.apache.commons.lang3.tuple.Pair;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;

import java.util.concurrent.TimeUnit;

import static com.apple.aml.stargate.common.utils.AppConfig.config;

public final class A3CacheUtils {
    // TODO : Need to use Dynamic TTL based on tokenInfo
    private static final Cache<Pair<Long, String>, A3TokenInfo> A3_TOKEN_CACHE = new Cache2kBuilder<Pair<Long, String>, A3TokenInfo>() {
    }.expireAfterWrite((config().hasPath("stargate.a3.cache.expiry") ? config().getInt("stargate.a3.cache.expiry") : 5), TimeUnit.HOURS).loader(key -> A3Utils.getA3TokenInfo(key.getLeft(), key.getRight())).build();

    private A3CacheUtils() {
    }

    public static String cachedA3Token(final long targetAppId) {
        return cachedA3Token(targetAppId, null);
    }

    public static String cachedA3Token(final long targetAppId, final String contextString) {
        Pair<Long, String> key = Pair.of(targetAppId, contextString);
        A3TokenInfo tokenInfo = A3_TOKEN_CACHE.get(key);
        return tokenInfo.getToken();
    }
}
