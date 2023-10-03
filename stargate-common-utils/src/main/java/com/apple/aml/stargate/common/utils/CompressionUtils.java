package com.apple.aml.stargate.common.utils;

import lombok.SneakyThrows;
import org.xerial.snappy.Snappy;

public final class CompressionUtils {
    private CompressionUtils() {

    }

    @SneakyThrows
    public static byte[] compress(final String compression, final byte[] input) {
        if ("snappy".equalsIgnoreCase(compression)) {
            byte[] output = Snappy.compress(input);
            return output;
        } else {
            return input; // TODO : Need to handle others
        }
    }

    @SneakyThrows
    public static byte[] uncompress(final String compression, final byte[] input) {
        if ("snappy".equalsIgnoreCase(compression)) {
            byte[] output = Snappy.uncompress(input);
            return output;
        } else {
            return input; // TODO : Need to handle others
        }
    }
}
