package com.apple.aml.stargate.pipeline.sdk.trigger;

import com.apple.aml.stargate.common.constants.PipelineConstants;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;

import java.io.Serializable;
import java.nio.charset.Charset;

public class BaseSequencer implements Serializable {

    public static Schema getSchema(PipelineConstants.ENVIRONMENT environment) {
        Schema.Parser parser = new Schema.Parser();
        String schemaString = getSchemaString().replaceAll("\\#\\{ENV\\}", environment.name().toLowerCase());
        return parser.parse(schemaString);
    }

    public static String getSchemaString() {
        try {
            String schemaString = IOUtils.resourceToString("/sequencer.avsc", Charset.defaultCharset());
            if (schemaString == null || schemaString.isBlank()) {
                throw new Exception("Could not load schemaString");
            }
            return schemaString;
        } catch (Exception e) {
            return "{\"type\":\"record\",\"name\":\"Sequencer\",\"namespace\":\"com.apple.aml.stargate.#{ENV}.internal\",\"fields\":[{\"name\":\"value\",\"type\":\"long\"},{\"name\":\"epoc\",\"type\":\"long\"},{\"name\":\"loadNo\",\"type\":[\"null\",\"long\"]}]}";
        }
    }
}
