package com.apple.aml.stargate.beam.sdk.io.file;

import com.apple.aml.stargate.beam.sdk.utils.FileWriterFns;
import com.apple.aml.stargate.common.io.RawByteArrayOutputStream;
import org.slf4j.Logger;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.apple.aml.stargate.beam.sdk.io.s3.S3IO.uploadS3BulkStream;
import static com.apple.aml.stargate.common.constants.CommonConstants.MAX_S3_BLOCK_SIZE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.utils.AppConfig.config;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;

public class InMemoryWritableByteChannel implements WritableByteChannel {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8; // ByteArrayOutputStream has limitation on max array size
    private static final int INITIAL_SIZE = (int) (10 * MAX_S3_BLOCK_SIZE);
    private final FileWriterFns.WriterKey writerKey;
    private final String fileName;
    private List<RawByteArrayOutputStream> streams;
    private RawByteArrayOutputStream stream;
    private boolean open = true;
    private boolean preferDirectCopy;

    public InMemoryWritableByteChannel(final FileWriterFns.WriterKey writerKey, final String fileName) {
        this.writerKey = writerKey;
        this.fileName = fileName;
        this.stream = new RawByteArrayOutputStream(INITIAL_SIZE);
        this.streams = new ArrayList<>();
        this.streams.add(this.stream);
        this.preferDirectCopy = config().getBoolean("stargate.sink.channel.inmemory.directcopy.enabled");
        LOGGER.debug("Created a new in-memory linked streams for", Map.of("fileName", fileName), writerKey.logMap());
    }

    @Override
    public synchronized int write(final ByteBuffer src) throws IOException {
        int size = src.remaining();
        if (size <= 0) {
            return 0;
        }
        try {
            if (preferDirectCopy && src.hasArray()) {
                int offset = src.arrayOffset() + src.position();
                byte[] bytes = src.array();
                int copySize = Math.min(MAX_ARRAY_SIZE - stream.size(), size);
                if (copySize <= 0) {
                    LOGGER.debug("Size exceeded max jvm allowed capacity. Will create a new linked stream", Map.of("fileName", fileName, "currentTotalSize", totalSize()), writerKey.logMap());
                    stream = new RawByteArrayOutputStream(INITIAL_SIZE);
                    streams.add(stream);
                    copySize = size;
                }
                stream.write(bytes, offset, copySize);
                if (copySize < size) {
                    LOGGER.debug("Size exceeded max jvm allowed capacity. Will create a new linked stream", Map.of("fileName", fileName, "currentTotalSize", totalSize()), writerKey.logMap());
                    stream = new RawByteArrayOutputStream(INITIAL_SIZE);
                    streams.add(stream);
                    stream.write(bytes, offset + copySize, size - copySize);
                }
                return size;
            }
            int i = 0;
            while (src.hasRemaining()) {
                if (stream.size() >= MAX_ARRAY_SIZE) {
                    LOGGER.debug("Size exceeded max jvm allowed capacity. Will create a new linked stream", Map.of("fileName", fileName, "currentTotalSize", totalSize()), writerKey.logMap());
                    stream = new RawByteArrayOutputStream(INITIAL_SIZE);
                    streams.add(stream);
                }
                stream.write(src.get());
                i++;
            }
            return i;
        } catch (Error e) {
            LOGGER.error("Error writing to in-memory stream", Map.of(ERROR_MESSAGE, e.getMessage(), "bufferSize", size, "streamSize", stream.size(), "streamCapacity", stream.rawBytePointer().length, "freeMemory", Runtime.getRuntime().freeMemory()), e);
            throw e;
        }
    }

    public long totalSize() {
        return streams.stream().map(s -> (long) s.size()).reduce((x, y) -> x + y).get();
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public void close() throws IOException {
        if (!open) {
            return;
        }
        if (fileName.startsWith("s3://") || fileName.startsWith("s3a://") || fileName.startsWith("s3n://")) {
            try {
                uploadS3BulkStream(fileName, streams, writerKey);
            } catch (Exception e) {
                LOGGER.warn("Could not upload stream to s3", Map.of("fileName", fileName, ERROR_MESSAGE, e.getMessage()), writerKey.logMap(), e);
                throw (e instanceof IOException ? (IOException) e : new IOException(e));
            }
        } else {
            throw new UnsupportedOperationException();
        }
        open = false;
        stream = null;
    }

}
