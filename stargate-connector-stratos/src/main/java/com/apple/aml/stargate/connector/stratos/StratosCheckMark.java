package com.apple.aml.stargate.connector.stratos;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.UnboundedSource;

import java.io.IOException;

@DefaultCoder(AvroCoder.class)
@Data
@AllArgsConstructor
public class StratosCheckMark implements UnboundedSource.CheckpointMark {
    @Override
    public void finalizeCheckpoint() throws IOException {

    }
}
