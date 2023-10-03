package com.apple.aml.stargate.connector.aci.kafka;

import com.apple.pie.queue.client.ext.schema.avro.KafkaAvroDeserializer;
import com.apple.pie.queue.client.ext.schema.header.SchemaDetails;
import com.apple.pie.queue.client.ext.schema.header.SchemaHeaderSerDe;
import com.apple.pie.queue.kafka.config.ConfigUtils;
import com.apple.pie.queue.kafka.crypto.DecryptionDeserializer;
import com.apple.pie.queue.kafka.crypto.DecryptionDeserializerConfigDef;
import com.apple.pie.queue.kafka.crypto.EncryptionSerializerConfigDef;
import com.apple.pie.queue.kafka.crypto.extension.CryptoExtensionSerDeserializer;
import com.apple.pie.queue.kafka.envelope.EnvelopeConfig;
import com.apple.pie.queue.kafka.envelope.PieDeserializer;
import com.apple.pie.queue.kafka.envelope.PieDeserializerConfigDef;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.UNKNOWN;
import static java.lang.Boolean.parseBoolean;

public class AvroACIDeserializer extends AvroDeserializer implements Deserializer<GenericRecord> {

    @Override
    @SuppressWarnings("unchecked")
    public Pair<Deserializer<GenericRecord>, Deserializer<GenericRecord>> deserializerInstances(final Map<String, ?> cMap, final boolean isKey) {
        Deserializer<GenericRecord> backingDeserializer;
        Deserializer<GenericRecord> fallbackDeserializer = null;
        Map<String, Object> configs = (Map<String, Object>) cMap;
        configs.put(PieDeserializerConfigDef.PAYLOAD_DESERIALIZER_CONFIG, KafkaAvroDeserializer.class);
        Deserializer deserializer = new PieDeserializerConfigDef(configs).getConfiguredInstance(PieDeserializerConfigDef.PAYLOAD_DESERIALIZER_CONFIG, Deserializer.class, configs);
        deserializer.configure(configs, isKey);
        Object decryptionEnabled = configs.get(DecryptionDeserializerConfigDef.TRY_TO_DECRYPT_MESSAGES_CONFIG);
        if (decryptionEnabled != null && parseBoolean(decryptionEnabled + "")) {
            Map<String, Object> fallbackConfigs = new HashMap<>(configs);
            fallbackConfigs.remove(DecryptionDeserializerConfigDef.TRY_TO_DECRYPT_MESSAGES_CONFIG);
            fallbackDeserializer = new PieDeserializer<GenericRecord>(deserializer, fallbackConfigs);
            fallbackDeserializer.configure(fallbackConfigs, isKey);
            configs.put(EnvelopeConfig.PAYLOAD_DESERIALIZER_CONFIG, DecryptionDeserializer.class);
            configs.put(EncryptionSerializerConfigDef.PAYLOAD_SERIALIZER_CONFIG, KafkaAvroDeserializer.class);
            configs.put(DecryptionDeserializerConfigDef.PAYLOAD_DESERIALIZER_CONFIG, KafkaAvroDeserializer.class);
            Object cryptoSerDeValue = configs.get(EnvelopeConfig.EXTENSION_SERDE_CONFIG);
            boolean appendCryptoSerDe;
            if (cryptoSerDeValue instanceof String) {
                appendCryptoSerDe = Arrays.stream(((String) cryptoSerDeValue).split(",")).noneMatch(s -> CryptoExtensionSerDeserializer.class.getName().equals(s));
            } else if (cryptoSerDeValue instanceof Collection) {
                appendCryptoSerDe = ((Collection<Object>) cryptoSerDeValue).stream().noneMatch(o -> CryptoExtensionSerDeserializer.class.equals(o) || CryptoExtensionSerDeserializer.class.getName().equals(o));
            } else {
                appendCryptoSerDe = true;
            }
            if (appendCryptoSerDe) ConfigUtils.appendToClassList(configs, EnvelopeConfig.EXTENSION_SERDE_CONFIG, CryptoExtensionSerDeserializer.class);
            deserializer = new DecryptionDeserializer<GenericRecord>(deserializer, configs);
        }
        backingDeserializer = new PieDeserializer<GenericRecord>(deserializer, configs);
        backingDeserializer.configure(configs, isKey);
        return Pair.of(backingDeserializer, fallbackDeserializer);
    }

    @Override
    public Pair<String, Integer> schemaDetails(final Headers headers) {
        try {
            Optional<SchemaDetails> schemaDetailsOptional = SchemaHeaderSerDe.getSchemaDetails(headers, Map.of());
            SchemaDetails schemaDetails = schemaDetailsOptional.get();
            return Pair.of(String.valueOf(schemaDetails.getSchemaName()), (int) schemaDetails.getSchemaVersion());
        } catch (Exception e) {
            return Pair.of(UNKNOWN, -1);
        }
    }


}
