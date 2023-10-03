package com.apple.aml.stargate.connector.iceberg;

import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.options.IcebergOpsOptions;
import com.apple.aml.stargate.common.options.SchemaLevelOptions;
import freemarker.template.Configuration;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.NestedField;
import org.slf4j.Logger;
import org.slf4j.event.Level;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.apple.aml.stargate.beam.sdk.io.hadoop.HdfsIO.HIVE_WAREHOUSE_KEY;
import static com.apple.aml.stargate.beam.sdk.printers.LogFns.log;
import static com.apple.aml.stargate.common.constants.CommonConstants.Expressions.TABLE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SOURCE_SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_IN;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricStages.ELEMENTS_OUT;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.PipelineConstants.DEFAULT_MAPPING;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.freeMarkerConfiguration;
import static com.apple.aml.stargate.common.utils.FreemarkerUtils.loadFreemarkerTemplate;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.connector.iceberg.IcebergIO.icebergCatalog;
import static com.apple.aml.stargate.pipeline.sdk.ts.BaseFreemarkerEvaluator.evaluateFreemarker;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.counter;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.fetchSchemaWithLocalFallback;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.hiveConfiguration;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.incCounters;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.isDynamicKerberosLoginEnabled;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.performKerberosLogin;
import static com.apple.jvm.commons.util.Strings.isBlank;

public class IcebergTableCreator extends DoFn<KV<String, GenericRecord>, KV<String, GenericRecord>> implements Serializable {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private final String nodeName;
    private final String nodeType;
    private final Level logPayloadLevel;
    private final String basePath;
    private final String dbPath;
    private final String schemaId;
    private final String schemaReference;
    private final Map<String, Object> tableProperties;
    private final PipelineConstants.CATALOG_TYPE catalogType;
    private final Map<String, Object> catalogProperties;
    private final Map<String, SchemaLevelOptions> mappings;
    private final SchemaLevelOptions defaultOptions;
    private final boolean enableLocation;
    private final String schemaIdTemplateName;
    private final boolean kerberosLoginEnabled;
    private transient ConcurrentHashMap<String, Map<String, String>> namespaceMap;
    private ConcurrentHashMap<String, String> tableIdTemplateNameMap;
    private Catalog catalog;
    private transient Configuration configuration;

    public IcebergTableCreator(final String nodeName, final String nodeType, final IcebergOpsOptions options) {
        LOGGER.debug("Creating Iceberg table with options={}", options);
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        this.logPayloadLevel = options.logPayloadLevel();
        this.schemaReference = options.getSchemaReference();
        this.schemaId = options.getSchemaId();
        this.mappings = options.mappings();
        this.defaultOptions = this.mappings.get(DEFAULT_MAPPING);
        this.basePath = options.basePath();
        this.dbPath = options.getDbPath();
        this.tableProperties = options.getTableProperties();
        this.catalogType = options.catalogType();
        this.catalogProperties = options.getCatalogProperties();
        this.enableLocation = options.enableLocation();
        this.kerberosLoginEnabled = isDynamicKerberosLoginEnabled();
        this.schemaIdTemplateName = nodeName + "~table-creator-schemaId";
        LOGGER.debug("Initialized constructor of IcebergTableCreator");
    }

    @Setup
    public void setup() throws Exception {
        if (kerberosLoginEnabled) performKerberosLogin();
        namespaceMap = new ConcurrentHashMap<>();
        catalog = icebergCatalog(catalogType, catalogProperties, hiveConfiguration());
        tableIdTemplateNameMap = new ConcurrentHashMap<>();
        configuration();
    }

    @SuppressWarnings("unchecked")
    private Configuration configuration() {
        if (configuration != null) {
            return configuration;
        }
        loadFreemarkerTemplate(schemaIdTemplateName, isBlank(schemaId) ? "${schema.fullName}" : schemaId.trim());
        configuration = freeMarkerConfiguration();
        return configuration;
    }

    @StartBundle
    public void startBundle(StartBundleContext context) {
        if (namespaceMap == null) {
            namespaceMap = new ConcurrentHashMap<>();
        }
    }

    @SuppressWarnings("unchecked")
    @ProcessElement
    public void processElement(@Element final KV<String, GenericRecord> kv, final ProcessContext ctx) throws Exception {
        log(logPayloadLevel, nodeName, nodeType, kv);
        Configuration config = configuration();
        GenericRecord record = kv.getValue();
        String schemaId = evaluateFreemarker(config, schemaIdTemplateName, kv.getKey(), kv.getValue(), record.getSchema());
        counter(nodeName, nodeType, schemaId, ELEMENTS_IN).inc();
        LOGGER.debug("SchemaId derived as", Map.of(SCHEMA_ID, schemaId, NODE_NAME, nodeName, NODE_TYPE, nodeType));
        Schema schema = fetchSchemaWithLocalFallback(schemaReference, schemaId);
        SchemaLevelOptions options = mappings.getOrDefault(schemaId, defaultOptions);
        String templateName = tableIdTemplateNameMap.computeIfAbsent(schemaId, s -> {
            String name = nodeName + "~" + s + "~~table-creator-tableId";
            loadFreemarkerTemplate(name, isBlank(options.getTableName()) ? TABLE_NAME : options.getTableName().trim());
            return name;
        });
        String tableId = evaluateFreemarker(config, templateName, kv.getKey(), kv.getValue(), schema);
        LOGGER.debug("Table derived as", Map.of(SCHEMA_ID, schemaId, "tableId", tableId, NODE_NAME, nodeName, NODE_TYPE, nodeType));
        Table table = getOrCreateTable(nodeName, nodeType, options.getDbName(), tableId, schema, mappings.getOrDefault(schema.getFullName(), defaultOptions), namespaceMap, catalog, dbPath, basePath, tableProperties, enableLocation);
        LOGGER.debug("Table created/loaded successfully", Map.of(SCHEMA_ID, schemaId, "tableId", tableId, "location", table.location(), NODE_NAME, nodeName, NODE_TYPE, nodeType));
        incCounters(nodeName, nodeType, schemaId, ELEMENTS_OUT, TABLE_NAME, tableId, SOURCE_SCHEMA_ID, schemaId);
        ctx.output(kv);
    }

    @SuppressWarnings("unchecked")
    public static synchronized Table getOrCreateTable(final String nodeName, final String nodeType, final String dbName, final String tableId, final Schema schema, final SchemaLevelOptions options, final Map<String, Map<String, String>> namespaceMap, final Catalog catalog, final String dbPath, final String basePath, Map<String, Object> tableProperties, final boolean enableLocation) {
        final String schemaId = schema.getFullName();
        LOGGER.debug("Trying to load/create iceberg table", Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType, SCHEMA_ID, schemaId, "tableId", tableId));
        TableIdentifier tableIdentifier = TableIdentifier.of(dbName, tableId);
        String tablePath = enableLocation ? (basePath + "/" + tableId) : null;
        Table table;
        try {
            table = catalog.loadTable(tableIdentifier);
            LOGGER.debug("Iceberg table already exists. Load successful", Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType, SCHEMA_ID, schemaId, "tableName", table.name(), "location", table.location(), "tableInfo", table.toString()));
            updateSchemaIfNecessary(table, AvroSchemaUtil.toIceberg(schema)); // TODO : Need to handle auto version upgrade rather than job restart
            table = catalog.loadTable(tableIdentifier);
        } catch (NoSuchTableException | NotFoundException ne) {
            LOGGER.debug("Creating a new iceberg table", Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType, SCHEMA_ID, schemaId, "tableId", tableId));
            validateNamespace(nodeName, nodeType, options.getDbName(), namespaceMap, catalog, dbPath);
            table = IcebergUtils.createTable(nodeName, nodeType, catalog, schema, tableIdentifier, tablePath, (Map<String, String>) (Map) tableProperties, options);
            LOGGER.debug("Iceberg table created successfully for", Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType, SCHEMA_ID, schemaId, "tableName", table.name(), "location", table.location(), "tableInfo", table.toString()));
        } catch (Exception e) {
            LOGGER.error("Error while creating iceberg table", Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType, SCHEMA_ID, schemaId, "tableId", tableId, ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
            throw e;
        }
        return table;
    }

    public static void validateNamespace(final String nodeName, final String nodeType, final String dbName, final Map<String, Map<String, String>> namespaceMap, final Catalog iCatalog, final String dbPath) {
        if (!(iCatalog instanceof SupportsNamespaces)) {
            return;
        }
        SupportsNamespaces catalog = (SupportsNamespaces) iCatalog;
        namespaceMap.computeIfAbsent(dbName, d -> {
            final Namespace namespace = Namespace.of(dbName);
            try {
                return catalog.loadNamespaceMetadata(namespace);
            } catch (NoSuchNamespaceException | NotFoundException e) {
                try {
                    LOGGER.debug("Creating a new iceberg namespace", Map.of("namespace", namespace, NODE_NAME, nodeName, NODE_TYPE, nodeType));
                    if ((dbPath == null)) {
                        catalog.createNamespace(namespace);
                    } else {
                        catalog.createNamespace(namespace, Map.of(HIVE_WAREHOUSE_KEY, dbPath));
                    }
                    LOGGER.debug("Iceberg namespace created successfully for", Map.of("namespace", namespace, "dbName", dbName, NODE_NAME, nodeName, NODE_TYPE, nodeType));
                    return catalog.loadNamespaceMetadata(namespace);
                } catch (AlreadyExistsException ae) {
                    return catalog.loadNamespaceMetadata(namespace);
                }
            }
        });
    }

    public static void updateSchemaIfNecessary(final Table table, final org.apache.iceberg.Schema newSchema) {
        org.apache.iceberg.Schema currentSchema = table.schema();

        if (!currentSchema.equals(newSchema)) {
            UpdateSchema updateSchema = table.updateSchema();

            Map<Integer, NestedField> currentFields = fieldMap(currentSchema);
            Map<Integer, NestedField> newFields = fieldMap(newSchema);

            for (Map.Entry<Integer, NestedField> newFieldEntry : newFields.entrySet()) {
                if (!currentFields.containsKey(newFieldEntry.getKey())) {
                    NestedField newField = newFieldEntry.getValue();
                    updateSchema.addColumn(newField.name(), newField.type());
                }
            }

            for (Integer currentFieldId : currentFields.keySet()) {
                if (!newFields.containsKey(currentFieldId)) {
                    NestedField oldField = currentFields.get(currentFieldId);
                    updateSchema.deleteColumn(oldField.name());
                }
            }

            for (Map.Entry<Integer, NestedField> newFieldEntry : newFields.entrySet()) {
                NestedField newField = newFieldEntry.getValue();
                NestedField currentField = currentFields.get(newField.fieldId());
                if (currentField != null && !currentField.type().equals(newField.type())) {
                    if (newField.type().isPrimitiveType()) {
                        updateSchema.updateColumn(newField.name(), (Type.PrimitiveType) newField.type());
                    } else {
                        //TODO to figure out how to handle nested structure, maybe request for future usecase
                        //for now blindy delete and create
                        updateSchema.deleteColumn(currentField.name());
                        updateSchema.addColumn(newField.name(), newField.type());
                    }
                }
            }
            updateSchema.commit();
        }
    }

    private static Map<Integer, NestedField> fieldMap(final org.apache.iceberg.Schema schema) {
        List<NestedField> fields = schema.columns();
        Map<Integer, NestedField> fieldMap = new HashMap<>();
        for (NestedField field : fields) {
            fieldMap.put(field.fieldId(), field);
        }
        return fieldMap;
    }
}
