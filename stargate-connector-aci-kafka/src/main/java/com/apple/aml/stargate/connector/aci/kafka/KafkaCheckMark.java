package com.apple.aml.stargate.connector.aci.kafka;

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
public class KafkaCheckMark implements UnboundedSource.CheckpointMark {
    private String topic;
    private int partition;
    private long offset;
    private long waterMark;
    @JsonIgnore
    @AvroIgnore
    private Optional<KafkaReader> reader;

    @Override
    public void finalizeCheckpoint() throws IOException {
        reader.ifPresent(r -> r.finalizeCheckpointMarkAsync(this));
    }
}
