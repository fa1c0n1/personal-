package com.apple.aml.stargate.beam.sdk.values;

import com.apple.aml.stargate.common.pojo.ErrorRecord;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithFailures;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import java.util.List;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.common.nodes.StargateNode.nodeName;

public class SCollection<T> {

    public enum TAG_NAMES {
        records, errors
    }

    public static final TupleTag<KV<String, ErrorRecord>> ERROR_TAG = new TupleTag<>(TAG_NAMES.errors.name()) {
    };
    public static final TupleTag<KV<String, GenericRecord>> OUTPUT_TAG = new TupleTag<>(TAG_NAMES.records.name()) {
    };
    public static final TupleTag<KV<String, String>> KV_STRING_VS_STRING_TAG = new TupleTag<>() {
    };
    public static final TupleTag<GenericRecord> GENERIC_RECORD_TAG = new TupleTag<>() {
    };
    public static final TupleTag<KV<String, KV<String, GenericRecord>>> GENERIC_RECORD_GROUP_TAG = new TupleTag<>() {
    };

    private final PCollection<T> collection;
    private final PCollectionList<KV<String, ErrorRecord>> errors;

    private SCollection(final PCollection<T> collection, final PCollectionList<KV<String, ErrorRecord>> errors) {
        this.collection = collection;
        this.errors = errors;
    }

    public static <O> SCollection<O> apply(final Pipeline pipeline, final String name, final PTransform<? super PBegin, PCollection<O>> root) {
        return of(pipeline, pipeline.apply(name, root));
    }

    public static <T> SCollection<T> of(final Pipeline pipeline, final PCollection<T> collection) {
        return of(pipeline, collection, PCollectionList.empty(pipeline));
    }

    public static <T> SCollection<T> of(final Pipeline pipeline, final PCollection<T> collection, final PCollectionList<KV<String, ErrorRecord>> errors) {
        return new SCollection<>(collection, errors);
    }

    public static <T> SCollection<T> flatten(final String name, final List<SCollection<T>> collections) {
        PCollection<T> flatternedCollections = PCollectionList.of(collections.stream().map(c -> c.collection).collect(Collectors.toList())).apply(name, Flatten.pCollections());
        PCollection<KV<String, ErrorRecord>> flatternedErrors = PCollectionList.of(collections.stream().map(c -> c.errors).flatMap(l -> l.getAll().stream()).collect(Collectors.toList())).apply(nodeName(name, "errors"), Flatten.pCollections());
        return new SCollection<>(flatternedCollections, PCollectionList.of(flatternedErrors));
    }

    public PCollection<T> collection() {
        return collection;
    }

    public WithFailures.Result<PCollection<T>, KV<String, ErrorRecord>> result() {
        return WithFailures.Result.of(collection, errors());
    }

    public PCollection<KV<String, ErrorRecord>> errors() {
        return errors.apply("flatten-errors", Flatten.pCollections());
    }

    public PCollectionList<KV<String, ErrorRecord>> getErrors() {
        return errors;
    }

    public SCollection<KV<String, GenericRecord>> apply(final String name, final ParDo.MultiOutput<T, T> parDo) {
        PCollectionTuple tuple = collection.apply(name, parDo);
        PCollection<KV<String, GenericRecord>> outputCollection = tuple.get(OUTPUT_TAG);
        PCollectionList<KV<String, ErrorRecord>> clubbedErrors = errors.and(tuple.get(ERROR_TAG));
        return new SCollection<>(outputCollection, clubbedErrors);
    }

    @SuppressWarnings("unchecked")
    public SCollection<KV<String, GenericRecord>> apply(final String name, final ParDo.SingleOutput<T, T> parDo) {
        PCollectionTuple tuple = collection.apply(name, parDo.withOutputTags((TupleTag<T>) OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));
        PCollection<KV<String, GenericRecord>> outputCollection = tuple.get(OUTPUT_TAG);
        PCollectionList<KV<String, ErrorRecord>> clubbedErrors = errors.and(tuple.get(ERROR_TAG));
        return new SCollection<>(outputCollection, clubbedErrors);
    }

    public SCollection<KV<String, T>> group(final String name, final DoFn<T, KV<String, T>> func, final TupleTag<KV<String, T>> outputTag) {
        return apply(name, func, outputTag);
    }

    public <O> SCollection<O> apply(final String name, final DoFn<T, O> func, final TupleTag<O> outputTag) {
        return apply(name, func, outputTag, null);
    }

    @SuppressWarnings("unchecked")
    public <O> SCollection<O> apply(final String name, final DoFn<T, O> func, final TupleTag<O> outputTag, final Coder coder) {
        PCollectionTuple tuple = collection.apply(name, ParDo.of(func).withOutputTags(outputTag, TupleTagList.of(ERROR_TAG)));
        PCollection<O> outputCollection = tuple.get(outputTag);
        if (coder != null) outputCollection = outputCollection.setCoder(coder);
        PCollectionList<KV<String, ErrorRecord>> clubbedErrors = errors.and(tuple.get(ERROR_TAG));
        return new SCollection<>(outputCollection, clubbedErrors);
    }

    public SCollection<KV<String, GenericRecord>> apply(final String name, final DoFn<T, KV<String, GenericRecord>> func) {
        return apply(name, func, OUTPUT_TAG);
    }

    @SuppressWarnings("unchecked")
    public <O> SCollection<O> apply(final String name, final PTransform<PCollection<T>, PCollection<T>> transform) {
        PCollection<T> outputCollection = collection.apply(name, transform);
        return (SCollection<O>) new SCollection<>(outputCollection, errors);
    }

    public SCollection<List<KV<String, GenericRecord>>> list(final String name, final PTransform<PCollection<T>, PCollection<List<KV<String, GenericRecord>>>> listTransform) {
        PCollection<List<KV<String, GenericRecord>>> outputCollection = collection.apply(name, listTransform);
        return new SCollection<>(outputCollection, errors);
    }

    public SCollection<KV<String, GenericRecord>> done(final String name, final PTransform<PCollection<T>, PDone> writer) {
        collection.apply(name, writer);
        return new SCollection<>(null, errors);
    }

    public SCollection<Row> sql(final String name, final SqlTransform query) {
        return new SCollection<>(collection.apply(name, query), errors);
    }
}
