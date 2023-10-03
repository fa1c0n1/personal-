package com.apple.aml.stargate.beam.sdk.formatters;

import lombok.SneakyThrows;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO.Write.FileNaming;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;

import static com.apple.aml.stargate.common.constants.CommonConstants.SPECIAL_DELIMITER_CAP_REGEX;

public final class GenericFileNamer implements FileNaming {
    private static final long serialVersionUID = 1L;
    final String partitionValue;

    public GenericFileNamer(final String partitionValue) {
        this.partitionValue = partitionValue;
    }

    @SneakyThrows
    @Override
    public String getFilename(final BoundedWindow window, final PaneInfo pane, final int numShards, final int shardIndex, final Compression compression) {
        String[] tokens = this.partitionValue.split(SPECIAL_DELIMITER_CAP_REGEX);
        return String.format("%s%s/%s-%s-%s.%s", tokens[0], tokens[3], window.maxTimestamp().getMillis(), shardIndex, pane.getIndex(), tokens[1]);
    }
}
