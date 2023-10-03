package com.apple.aml.stargate.pipeline.pojo;

import com.apple.aml.stargate.common.configs.NodeConfig;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.exceptions.InvalidInputException;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.flink.streaming.api.EnrichedDataStream;
import lombok.SneakyThrows;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.lang.reflect.Method;

import static com.apple.aml.stargate.common.constants.CommonConstants.EMPTY_STRING;
import static com.apple.aml.stargate.common.utils.ClassUtils.getMatchingMethod;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJson;
import static com.apple.jvm.commons.util.Strings.isBlank;

public class FlinkVertx extends Vertx {
    private EnrichedDataStream output;
    private boolean init;
    private String type; // read/writ/transform
    private Class runtimeClass;
    private Object runtimeObject;
    private Object runtimeInitOutput;
    private Method runtimeMethod;
    private int runtimeMethodType = -1;

    public boolean init(final boolean init) {
        this.init = init;
        return this.init;
    }

    public boolean init() {
        return this.init;
    }

    public EnrichedDataStream output() {
        return this.output;
    }

    public EnrichedDataStream output(final EnrichedDataStream output) {
        if (this.output != null) return this.output;
        this.output = output;
        return this.output;
    }

    @SuppressWarnings("unchecked")
    @SneakyThrows
    public void initRuntime(final StreamExecutionEnvironment pipeline) {
        Object object;
        if (value.getConfig() != null && value.getConfig().getClass().equals(runtimeClass)) {
            object = value.getConfig();
        } else {
            try {
                object = runtimeClass.getDeclaredConstructor(value.getClass()).newInstance(pipeline, this);
            } catch (Exception e) {
                try {
                    object = readJson((value.getConfig() == null) ? "{}" : jsonString(value.getConfig()), runtimeClass);
                    if (object == null) {
                        object = runtimeClass.getDeclaredConstructor().newInstance();
                    }
                } catch (Exception ex) {
                    object = runtimeClass.getDeclaredConstructor().newInstance();
                }
            }
        }
        if (object == null) throw new NoSuchMethodException();
        String initMethodName = "init" + (type.charAt(0) + EMPTY_STRING).toUpperCase() + type.substring(1);
        Object initMethodOutput = null;
        Method initMethod = getMatchingMethod(runtimeClass, initMethodName, value.getClass());
        if (initMethod == null) {
            initMethod = getMatchingMethod(runtimeClass, initMethodName);
            if (initMethod != null) {
                initMethodOutput = initMethod.invoke(object);
            }
        } else {
            initMethodOutput = initMethod.invoke(object, value);
        }
        String methodName = type.toLowerCase();
        Method method = null;
        Class[] params = new Class[]{value.getClass(), EnrichedDataStream.class};
        method = getMatchingMethod(runtimeClass, methodName, params);

        if (method == null) throw new NoSuchMethodException();
        this.runtimeObject = object;
        this.runtimeMethod = method;
        this.runtimeInitOutput = initMethodOutput;
    }

    @SuppressWarnings({"unchecked"})
    public EnrichedDataStream invokeRuntime(EnrichedDataStream enrichedDataStream) throws Exception {
        EnrichedDataStream output = enrichedDataStream;
        boolean preFuncApplied = false;
        if (value.getFilter() != null || value.getLogLevelIn() != null || value.isLog() || value.isSaveStateIn() || value.isDistinct() || !isBlank(value.getMetricsExpression())) {
            output = applyPreFunctions(enrichedDataStream);
            preFuncApplied = true;
        }

        Object[] args = new Object[2];
        args[0] = value;
        args[1] = output;
        output = (EnrichedDataStream) this.runtimeMethod.invoke(this.runtimeObject, args);

        output = applyPostFunctions(output, !preFuncApplied);
        return output;
    }

    @SneakyThrows
    @SuppressWarnings({"unchecked", "deprecation"})
    private EnrichedDataStream applyPostFunctions(EnrichedDataStream enrichedDataStream, final boolean applyMetrics) {
        return enrichedDataStream;
    }

    @SuppressWarnings("unchecked")
    private EnrichedDataStream applyPreFunctions(EnrichedDataStream enrichedDataStream) throws Exception {
        return enrichedDataStream;
    }

    public Class runtimeClass() {
        return runtimeClass;
    }

    @SneakyThrows
    public void initRuntimeClass(final String type, final String dagName, final PipelineOptions options) {
        if (runtimeClass != null) return;
        this.type = type;
        NodeConfig config = PipelineConstants.NODE_TYPE.nodeType(value.getType()).getConfig();
        switch (type.toLowerCase()) {
            case "read":
                runtimeClass = config.getFlinkReaderClass();
                break;
            case "write":
                runtimeClass = config.getFlinkWriterClass();
                break;
            default:
                runtimeClass = config.getFlinkTransformerClass();
        }
        if (runtimeClass == null)
            throw new InvalidInputException(String.format("%s - node is configured as reader node in dag %s, but %s is not supported for %s", value.getName(), dagName, value.getType(), type)).wrap();
        Method method = getMatchingMethod(runtimeClass, "init", StargateNode.class);
        if (method == null) return;
        method.invoke(this.runtimeObject, value);
    }

    @Override
    public String toString() {
        return "Vertex{" + this.getValue() + "}";
    }


}
