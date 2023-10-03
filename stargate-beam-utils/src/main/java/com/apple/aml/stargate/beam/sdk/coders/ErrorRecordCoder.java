package com.apple.aml.stargate.beam.sdk.coders;

import com.apple.aml.stargate.common.pojo.ErrorRecord;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ErrorRecordCoder extends AtomicCoder<ErrorRecord> {
    private static final long serialVersionUID = 1L;
    private final StringUtf8Coder stringUtf8Coder;
    private final AvroCoder avroCoder;
    private final SerializableCoder<Exception> exceptionCoder;

    private ErrorRecordCoder(final String schemaReference) {
        this(AvroCoder.of(schemaReference));
    }

    private ErrorRecordCoder(final AvroCoder avroCoder) {
        this.stringUtf8Coder = StringUtf8Coder.of();
        this.avroCoder = avroCoder;
        this.exceptionCoder = SerializableCoder.of(Exception.class);
    }

    public static ErrorRecordCoder of(final String schemaReference) {
        return new ErrorRecordCoder(schemaReference);
    }

    public static ErrorRecordCoder of(final AvroCoder avroCoder) {
        return new ErrorRecordCoder(avroCoder);
    }

    @Override
    public void encode(final ErrorRecord value, final OutputStream stream) throws CoderException, IOException {
        stringUtf8Coder.encode(value.getNodeName(), stream);
        stringUtf8Coder.encode(value.getNodeType(), stream);
        stringUtf8Coder.encode(value.getStage(), stream);
        stringUtf8Coder.encode(value.getKey(), stream);
        avroCoder.encode(value.getRecord(), stream);
        exceptionCoder.encode(value.getException(), stream);
        stringUtf8Coder.encode(value.getErrorMessage(), stream);
    }

    @Override
    public ErrorRecord decode(final InputStream stream) throws CoderException, IOException {
        ErrorRecord record = new ErrorRecord();
        record.setNodeName(stringUtf8Coder.decode(stream));
        record.setNodeType(stringUtf8Coder.decode(stream));
        record.setStage(stringUtf8Coder.decode(stream));
        record.setKey(stringUtf8Coder.decode(stream));
        record.setRecord(avroCoder.decode(stream));
        record.setException(exceptionCoder.decode(stream));
        record.setErrorMessage(stringUtf8Coder.decode(stream));
        return record;
    }
}
