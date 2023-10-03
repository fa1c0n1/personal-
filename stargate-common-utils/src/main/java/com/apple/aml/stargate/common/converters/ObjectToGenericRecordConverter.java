package com.apple.aml.stargate.common.converters;

import com.apple.aml.stargate.common.options.BaseOptions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.Serializable;
import java.util.Iterator;

import static com.apple.aml.stargate.common.constants.CommonConstants.EnvironmentVariables.APP_AVRO_DEFAULT_CONVERTER;
import static com.apple.aml.stargate.common.utils.AppConfig.env;
import static com.apple.jvm.commons.util.Strings.isBlank;

public interface ObjectToGenericRecordConverter extends Serializable {
    GenericRecord convert(final Object object) throws Exception;

    GenericRecord convert(final Object object, final String unstructuredFieldName) throws Exception;

    Iterator<GenericRecord> iterator(final Object object) throws Exception;

    enum CONVERTER_TYPE {
        jackson, lazy, unstructuredlazy, legacy, xml
    }

    CONVERTER_TYPE DEFAULT_CONVERTER_TYPE = CONVERTER_TYPE.valueOf(env(APP_AVRO_DEFAULT_CONVERTER, CONVERTER_TYPE.lazy.name()).trim().toLowerCase());

    static ObjectToGenericRecordConverter converter(final Schema schema, final BaseOptions options) {
        return converter(schema, isBlank(options.getAvroConverter()) ? DEFAULT_CONVERTER_TYPE : CONVERTER_TYPE.valueOf(options.getAvroConverter().trim().toLowerCase()));
    }

    static ObjectToGenericRecordConverter converter(final Schema schema) {
        return converter(schema, DEFAULT_CONVERTER_TYPE);
    }

    static ObjectToGenericRecordConverter converter(final Schema schema, final CONVERTER_TYPE type) {
        switch (type) {
            case jackson:
                return JacksonGenericRecordConverter.converter(schema);
            case unstructuredlazy:
                return LazyJsonToUnstructuredGenericRecordConverter.converter(schema);
            case legacy:
                return JsonToGenericRecordConverter.converter(schema);
            case xml:
                return XmlToGenericRecordConverter.converter(schema);
            default:
                return LazyJsonToGenericRecordConverter.converter(schema);
        }
    }
}
