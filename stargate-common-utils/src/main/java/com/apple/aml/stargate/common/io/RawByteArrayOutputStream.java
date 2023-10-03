package com.apple.aml.stargate.common.io;

import java.io.ByteArrayOutputStream;

public class RawByteArrayOutputStream extends ByteArrayOutputStream {
    public RawByteArrayOutputStream(final int size) {
        super(size);
    }

    public byte[] rawBytePointer() {
        return buf;
    }
}
