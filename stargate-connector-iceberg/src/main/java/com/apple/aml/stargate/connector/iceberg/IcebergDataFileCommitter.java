package com.apple.aml.stargate.connector.iceberg;

import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.apple.aml.stargate.common.exceptions.GenericException;
import com.apple.aml.stargate.common.options.IcebergOptions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.values.KV;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter.converter;
import static com.apple.aml.stargate.connector.iceberg.IcebergIO.icebergCatalog;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.getInternalSchema;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.hiveConfiguration;

public class IcebergDataFileCommitter extends Combine.CombineFn<KV<String, DataFile>, List<KV<String, DataFile>>, List<KV<String, GenericRecord>>> {

    private final PipelineConstants.ENVIRONMENT environment;
    private final Schema schema;
    private final ObjectToGenericRecordConverter converter;
    private final PipelineConstants.CATALOG_TYPE catalogType;
    private final Map<String, Object> catalogProperties;

    public IcebergDataFileCommitter(final PipelineConstants.ENVIRONMENT environment, final IcebergOptions options) {
        this.environment = environment;
        this.schema = getInternalSchema(environment, "filewriter.avsc", "{\"type\": \"record\",\"name\": \"FileDetails\",\"namespace\": \"com.apple.aml.stargate.#{ENV}.internal\",\"fields\": [{\"name\": \"fileName\",\"type\": \"string\"}]}");
        this.converter = converter(this.schema);
        this.catalogType = options.catalogType();
        this.catalogProperties = options.getCatalogProperties();
    }

    @Override
    public List<KV<String, DataFile>> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<KV<String, DataFile>> addInput(List<KV<String, DataFile>> mutableAccumulator, KV<String, DataFile> input) {
        mutableAccumulator.add(input);
        return mutableAccumulator;
    }

    @Override
    public List<KV<String, DataFile>> mergeAccumulators(Iterable<List<KV<String, DataFile>>> accumulators) {
        Iterator<List<KV<String, DataFile>>> itr = accumulators.iterator();
        if (itr.hasNext()) {
            List<KV<String, DataFile>> first = itr.next();
            while (itr.hasNext()) {
                first.addAll(itr.next());
            }
            return first;
        } else {
            return new ArrayList<>();
        }
    }

    @Override
    public List<KV<String, GenericRecord>> extractOutput(List<KV<String, DataFile>> datafiles) {
        Map<String, List<DataFile>> map = new HashMap<>();
        if (datafiles.isEmpty()) {
            return new ArrayList<>();
        }
        for (KV<String, DataFile> dataFileKV : datafiles) {
            List<DataFile> files = map.get(dataFileKV.getKey());
            if (files == null) {
                files = new ArrayList<>();
                map.put(dataFileKV.getKey(), files);
            }
            files.add(dataFileKV.getValue());
        }
        List<KV<String, GenericRecord>> fileDetails = new ArrayList<>();
        Catalog catalog = icebergCatalog(catalogType, catalogProperties, hiveConfiguration());
        try {
            for (Map.Entry<String, List<DataFile>> entry : map.entrySet()) {
                String[] tableNameSplit = entry.getKey().split("\\.");
                TableIdentifier tableIdentifier = TableIdentifier.of(tableNameSplit[0], tableNameSplit[1]);
                Table table = catalog.loadTable(tableIdentifier);
                final AppendFiles app = table.newAppend();
                for (DataFile datafile : entry.getValue()) {
                    app.appendFile(datafile);
                    fileDetails.add(KV.of(entry.getKey(), this.converter.convert(Map.of("fileName", datafile.path(), "recordCount", datafile.recordCount()))));
                }
                app.commit();
            }
        } catch (Exception e) {
            throw new GenericException("Error while committing iceberg files", map, e).wrap();
        }
        return fileDetails;
    }
}
