package com.apple.aml.stargate.common.utils;

import lombok.SneakyThrows;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.time.FastDateFormat;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

public final class TemplateUtils {
    private static final Random random = new Random(System.nanoTime());
    private static final FastDateFormat yyyyMMddHHmmss = FastDateFormat.getInstance("yyyyMMddHHmmss", TimeZone.getTimeZone(ZoneId.from(ZoneOffset.UTC)));
    private static final FastDateFormat yyyyMMdd = FastDateFormat.getInstance("yyyyMMdd", TimeZone.getTimeZone(ZoneId.from(ZoneOffset.UTC)));
    private static final FastDateFormat HHmmss = FastDateFormat.getInstance("HHmmss", TimeZone.getTimeZone(ZoneId.from(ZoneOffset.UTC)));
    private static final ConcurrentHashMap<String, FastDateFormat> FORMATTERS = new ConcurrentHashMap<>(Map.of("yyyyMMddHHmmss", yyyyMMddHHmmss, "yyyyMMdd", yyyyMMdd, "HHmmss", HHmmss));

    private TemplateUtils() {
    }

    public static String randomSha1Hex(final String inputString, final int limit) {
        return DigestUtils.sha1Hex(String.format("%s-%04d", inputString, random.nextInt(limit)));
    }

    public static String sha1Hex(final String inputString) {
        return DigestUtils.sha1Hex(inputString);
    }

    public static String randomPrefix(final String inputString, final int limit) {
        return Integer.toHexString(Math.abs(String.format("%02d-%s", random.nextInt(limit), inputString).hashCode()) % 1000000);
    }

    public static String yyyyMMddHHmmss(final long milliseconds) {
        return yyyyMMddHHmmss.format(milliseconds);
    }

    public static String yyyyMMdd(final long milliseconds) {
        return yyyyMMdd.format(milliseconds);
    }

    public static String HHmmss(final long milliseconds) {
        return HHmmss.format(milliseconds);
    }

    public static String formattedDate(final String format, final long milliseconds) {
        return formattedTime(format, milliseconds);
    }

    public static String formattedTime(final String format, final long milliseconds) {
        return FORMATTERS.computeIfAbsent(format, f -> FastDateFormat.getInstance(f, TimeZone.getTimeZone(ZoneId.from(ZoneOffset.UTC)))).format(milliseconds);
    }

    public static Instant now() {
        return Instant.now();
    }

    public static int stringHashCode(final String str) {
        return str.hashCode();
    }

    @SneakyThrows
    public static Map jsonMap(final Object obj) {
        if (obj == null) return null;
        return JsonUtils.readJson(obj.toString(), Map.class);
    }

    @SneakyThrows
    public static List jsonList(final Object obj) {
        if (obj == null) return null;
        return JsonUtils.readJson(obj.toString(), List.class);
    }

    @SneakyThrows
    public static long sleep(final long millis) {
        Thread.sleep(millis);
        return System.currentTimeMillis();
    }

}
