package com.apple.aml.stargate.beam.sdk.io.solr;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.avro.reflect.AvroIgnore;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.UnboundedSource;

import java.io.IOException;
import java.util.Optional;

@DefaultCoder(AvroCoder.class)
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SolrCheckMark implements UnboundedSource.CheckpointMark {
    private long start;
    private long end;
    private long waterMark;

    @JsonIgnore
    @AvroIgnore
    private Optional<SolrUnboundedReader> reader;

    @Override
    public void finalizeCheckpoint() throws IOException {

    }
}
