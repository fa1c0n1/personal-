package com.apple.aml.stargate.pipeline.pojo;

import com.apple.aml.stargate.beam.sdk.transforms.UtilFns;
import com.apple.aml.stargate.beam.sdk.ts.MetricsEvaluator;
import com.apple.aml.stargate.beam.sdk.ts.SaveState;
import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.beam.sdk.values.SMapCollection;
import com.apple.aml.stargate.common.configs.NodeConfig;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.exceptions.InvalidInputException;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.pojo.Holder;
import lombok.SneakyThrows;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.Reshuffle;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static com.apple.aml.stargate.beam.sdk.printers.LogFns.applyLogFns;
import static com.apple.aml.stargate.beam.sdk.utils.BeamUtils.applyKVFilters;
import static com.apple.aml.stargate.common.constants.CommonConstants.EMPTY_STRING;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.utils.AppConfig.environment;
import static com.apple.aml.stargate.common.utils.ClassUtils.getMatchingMethod;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJson;
import static com.apple.jvm.commons.util.Strings.isBlank;

public class BeamVertx extends Vertx {
    private Object output;
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

    public Object output() {
        return this.output;
    }

    public Object output(final Object output) {
        if (this.output != null) return this.output;
        this.output = output;
        return this.output;
    }

    @SuppressWarnings("unchecked")
    @SneakyThrows
    public void initRuntime(final Pipeline pipeline) {
        Object object;
        if (value.getConfig() != null && value.getConfig().getClass().equals(runtimeClass)) {
            object = value.getConfig();
        } else {
            try {
                object = runtimeClass.getDeclaredConstructor(Pipeline.class, value.getClass()).newInstance(pipeline, this);
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
        Class initMethodType = null;
        Object initMethodOutput = null;
        Method initMethod = getMatchingMethod(runtimeClass, initMethodName, Pipeline.class, value.getClass());
        if (initMethod == null) {
            initMethod = getMatchingMethod(runtimeClass, initMethodName);
            if (initMethod != null) {
                initMethodOutput = initMethod.invoke(object);
            }
        } else {
            initMethodOutput = initMethod.invoke(object, pipeline, value);
        }
        if (initMethod != null) {
            initMethodType = initMethod.getReturnType();
        }
        String methodName = type.toLowerCase();
        Method method = null;
        Class[] params;
        boolean include = !"read".equalsIgnoreCase(type);
        if (initMethodType != null && !initMethodType.equals(Void.TYPE)) {
            params = include ? new Class[]{Pipeline.class, value.getClass(), initMethodType, SCollection.class} : new Class[]{Pipeline.class, value.getClass(), initMethodType};
            method = getMatchingMethod(runtimeClass, methodName, params);
            runtimeMethodType = 1;
        }
        if (method == null) {
            params = include ? new Class[]{Pipeline.class, value.getClass(), SCollection.class} : new Class[]{Pipeline.class, value.getClass()};
            method = getMatchingMethod(runtimeClass, methodName, params);
            runtimeMethodType = 2;
        }
        if (method == null) {
            method = include ? getMatchingMethod(runtimeClass, methodName, SCollection.class) : getMatchingMethod(runtimeClass, methodName);
            runtimeMethodType = 3;
        }
        if (method == null) throw new NoSuchMethodException();
        this.runtimeObject = object;
        this.runtimeMethod = method;
        this.runtimeInitOutput = initMethodOutput;
    }

    @SuppressWarnings({"unchecked"})
    public Object invokeRuntime(final Pipeline pipeline, final Object... objects) throws Exception {
        Object[] args = new Object[this.runtimeMethod.getParameterCount()];
        int startIndex = -1;
        if (runtimeMethodType == 1) {
            args[0] = pipeline;
            args[1] = value;
            args[2] = this.runtimeInitOutput;
            startIndex = 3;
        } else if (runtimeMethodType == 2) {
            args[0] = pipeline;
            args[1] = value;
            startIndex = 2;
        } else if (runtimeMethodType == 3) {
            startIndex = 0;
        }
        int i = 0;
        for (; startIndex < args.length; startIndex++) {
            args[startIndex] = objects[i++];
        }
        boolean preFuncApplied = false;
        for (int x = 0; x < args.length; x++) {
            if (args[x] == null) continue;
            String nodeName = value.getName();
            SCollection collection;
            if (args[x] instanceof SCollection) {
                collection = (SCollection) args[x];
            } else if (args[x] instanceof SMapCollection) {
                String branchName = EMPTY_STRING;
                Map<String, SCollection> smap = ((SMapCollection) args[x]).collections();
                if (nodeName.contains("~")) {
                    String[] split = nodeName.split("~");
                    if (split.length >= 3) throw new InvalidInputException("branched nodeNames cannot have more than 2 tokens!!", Map.of(NODE_NAME, nodeName));
                    branchName = split[1];
                }
                collection = smap.get(branchName);
            } else {
                continue;
            }
            if (collection == null) continue;
            if (value.getFilter() != null || value.getLogLevelIn() != null || value.isLog() || value.isSaveStateIn() || value.isDistinct() || !isBlank(value.getMetricsExpression())) {
                collection = applyPreFunctions(pipeline, collection);
                preFuncApplied = true;
            }
            args[x] = collection;
        }
        Object output = this.runtimeMethod.invoke(this.runtimeObject, args);
        if (output instanceof SCollection) {
            output = applyPostFunctions(pipeline, (SCollection) output, !preFuncApplied);
        } else if (output instanceof SMapCollection) {
            boolean applyMetrics = !preFuncApplied;
            SMapCollection smap = ((SMapCollection) output);
            Collections.unmodifiableCollection((Set<String>) smap.collections().keySet()).stream().forEach(branch -> smap.update(branch, applyPostFunctions(pipeline, (SCollection) smap.collections().get(branch), applyMetrics)));
        }
        return output;
    }

    @SneakyThrows
    @SuppressWarnings({"unchecked", "deprecation"})
    private SCollection applyPostFunctions(final Pipeline pipeline, SCollection collection, final boolean applyMetrics) {
        if (applyMetrics && !isBlank(value.getMetricsExpression())) collection = collection.apply(value.name("metrics"), new MetricsEvaluator(value.name("metrics"), value.getType(), value.getMetricsExpression()));
        if (value.getLogLevelOut() != null) collection = applyLogFns(value.name("post"), value.getType(), value.getLogLevelOut(), collection);
        if (value.isSaveStateOut()) {
            String parDoName = value.name("save-state-out");
            SaveState saveState = new SaveState();
            saveState.initCommon(pipeline, null, parDoName, value.getType(), environment());
            collection = collection.apply(parDoName, saveState);
        }
        if (value.getMaxTps() >= 1) collection = collection.apply(value.name("rate-limit"), UtilFns.rateLimiter(value.getMaxTps()));
        if (value.isShuffle()) collection = collection.apply(value.name("shuffle-output"), Reshuffle.viaRandomKey());
        return collection;
    }

    @SuppressWarnings("unchecked")
    private SCollection applyPreFunctions(final Pipeline pipeline, SCollection collection) throws Exception {
        if (!isBlank(value.getMetricsExpression())) collection = collection.apply(value.name("metrics"), new MetricsEvaluator(value.name("metrics"), value.getType(), value.getMetricsExpression()));
        if (value.getLogLevelIn() != null) collection = applyLogFns(value.name("pre"), value.getType(), value.getLogLevelIn(), collection);
        if (value.getFilter() != null) collection = applyKVFilters(value.name("filter"), value.getType(), value.getFilter(), collection);
        if (value.isDistinct()) collection = collection.apply(value.name("distinct"), Distinct.create());
        if (value.isLog()) collection = applyLogFns(value.name("log"), value.getType(), "INFO", collection);
        if (value.isSaveStateIn()) {
            String parDoName = value.name("save-state-in");
            SaveState saveState = new SaveState();
            saveState.initCommon(pipeline, null, parDoName, value.getType(), environment());
            collection = collection.apply(parDoName, saveState);
        }
        return collection;
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
                runtimeClass = config.getReaderClass();
                break;
            case "write":
                runtimeClass = config.getWriterClass();
                break;
            default:
                runtimeClass = config.getTransformerClass();
        }
        if (runtimeClass == null) throw new InvalidInputException(String.format("%s - node is configured as reader node in dag %s, but %s is not supported for %s", value.getName(), dagName, value.getType(), type)).wrap();
        Method method = getMatchingMethod(runtimeClass, "init", PipelineOptions.class, StargateNode.class);
        if (method == null) return;
        method.invoke(null, options, value);
    }

    @Override
    public String toString() {
        return "Vertex{" + this.getValue() + "}";
    }
}
