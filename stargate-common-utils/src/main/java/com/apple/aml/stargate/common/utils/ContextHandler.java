package com.apple.aml.stargate.common.utils;

import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.apache.avro.Schema;

import java.io.Serializable;

public class ContextHandler implements Serializable {
    private static final ThreadLocal<Context> context = new ThreadLocal<>();

    public static void setContext(final Context ctx) {
        context.set(ctx);
    }

    public static void appendContext(final Context ctx) {
        Context current = context.get();
        if (current == null) {
            context.set(ctx);
            return;
        }
        current.setNodeName(ctx.nodeName);
        current.setNodeType(ctx.nodeType);
        current.setSchema(ctx.schema);
        current.setInputSchemaId(ctx.inputSchemaId);
        current.setOutputSchemaId(ctx.outputSchemaId);
        current.setConverter(ctx.converter);
    }

    public static void clearContext() {
        context.remove();
    }

    public static Context ctx() {
        return context.get();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @SuperBuilder
    public static class Context {
        protected String nodeName;
        protected String nodeType;
        protected Schema schema;
        protected String inputSchemaId;
        protected String outputSchemaId;
        protected ObjectToGenericRecordConverter converter;
    }
}
