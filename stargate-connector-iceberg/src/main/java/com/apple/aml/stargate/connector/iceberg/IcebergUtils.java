package com.apple.aml.stargate.connector.iceberg;

import com.apple.aml.stargate.common.options.SchemaLevelOptions;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.DEFAULT_DELIMITER;
import static com.apple.aml.stargate.common.constants.CommonConstants.EMPTY_STRING;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static java.lang.Integer.parseInt;

public final class IcebergUtils {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    private IcebergUtils() {
    }

    public static Table createTable(final String nodeName, final String nodeType, final Catalog catalog, final org.apache.avro.Schema schema, final TableIdentifier tableIdentifier, final String tablePath, final Map<String, String> tableProperties, final SchemaLevelOptions options) {
        Table table;
        Map logMap = Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType, "catalog", catalog, "schema", schema, "tableIdentifier", tableIdentifier, "tablePath", String.valueOf(tablePath), "tableProperties", tableProperties, "partitionColumn", String.valueOf(options.getPartitionColumn()), "sortColumn", String.valueOf(options.getSortColumn()), "buckets", String.valueOf(options.getBuckets()));
        LOGGER.debug("Starting to create iceberg table", logMap);
        try {
            final org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(schema);
            PartitionSpec.Builder builder = PartitionSpec.builderFor(icebergSchema);
            if (options.getPartitionColumn() != null && !options.getPartitionColumn().trim().isBlank()) {
                builder = builder.identity(options.getPartitionColumn().trim());
            }
            //Adding time based transforms for partition spec with default as identity. TODO can remove above duplicate implementation for single partition column.
            if (options.getPartitionTransforms() != null && !options.getPartitionTransforms().isEmpty()) {
                for (final String partitionTransform : options.getPartitionTransforms()) {
                    String[] pair = partitionTransform.trim().split(DEFAULT_DELIMITER);
                    String partitionColumn = pair[0].trim();
                    String partitionTransformType = EMPTY_STRING;
                    if (pair.length > 1) partitionTransformType = pair[1].trim();
                    switch (partitionTransformType) {
                        case "year":
                            builder = builder.year(partitionColumn);
                            break;
                        case "month":
                            builder = builder.month(partitionColumn);
                            break;
                        case "day":
                            builder = builder.day(partitionColumn);
                            break;
                        case "hour":
                            builder = builder.hour(partitionColumn);
                            break;
                        default:
                            builder = builder.identity(partitionColumn);
                    }
                }
            }
            if (options.getBuckets() != null && !options.getBuckets().isEmpty()) {
                for (final String bucket : options.getBuckets()) {
                    String[] pair = bucket.trim().split(DEFAULT_DELIMITER);
                    String bucketName = pair[0].trim();
                    int numOfBuckets = parseInt(pair[1].trim());
                    builder = builder.bucket(bucketName, numOfBuckets);
                }
            }
            PartitionSpec spec = builder.build();
            Catalog.TableBuilder tableBuilder = catalog.buildTable(tableIdentifier, icebergSchema).withPartitionSpec(spec).withProperties(tableProperties);
            if (!isBlank(tablePath)) {
                tableBuilder = tableBuilder.withLocation(tablePath);
            }
            if (options.getSortColumn() != null && !options.getSortColumn().trim().isBlank()) {
                String[] sortPair = options.getSortColumn().trim().split(DEFAULT_DELIMITER);
                SortDirection direction = sortPair.length == 1 ? SortDirection.ASC : ("desc".equalsIgnoreCase(sortPair[1]) ? SortDirection.DESC : SortDirection.ASC);
                NullOrder nullOrder = sortPair.length <= 2 ? NullOrder.NULLS_LAST : ("first".equalsIgnoreCase(sortPair[2]) ? NullOrder.NULLS_FIRST : NullOrder.NULLS_LAST);
                SortOrder sortOrder = SortOrder.builderFor(icebergSchema).sortBy(sortPair[0].trim(), direction, nullOrder).build();
                tableBuilder = tableBuilder.withSortOrder(sortOrder);
            }
            //Adding multiple sort columns implementation. TODO can remove above duplicate implementation for single sort column.
            if (options.getSortColumns() != null && !options.getSortColumns().isEmpty()) {
                for (final String sortColumnDef : options.getSortColumns()) {
                    String[] sortPair = sortColumnDef.trim().split(DEFAULT_DELIMITER);
                    SortDirection direction = sortPair.length == 1 ? SortDirection.ASC : ("desc".equalsIgnoreCase(sortPair[1]) ? SortDirection.DESC : SortDirection.ASC);
                    NullOrder nullOrder = sortPair.length <= 2 ? NullOrder.NULLS_LAST : ("first".equalsIgnoreCase(sortPair[2]) ? NullOrder.NULLS_FIRST : NullOrder.NULLS_LAST);
                    SortOrder sortOrder = SortOrder.builderFor(icebergSchema).sortBy(sortPair[0].trim(), direction, nullOrder).build();
                    tableBuilder = tableBuilder.withSortOrder(sortOrder);
                }
            }
            table = tableBuilder.create();
            LOGGER.info("Table created successfully", logMap, Map.of("schema", table.schema().toString()));
        } catch (AlreadyExistsException ae) {
            table = catalog.loadTable(tableIdentifier);
            LOGGER.debug("Iceberg table already exists. Nothing to do here..", logMap);
        } catch (Exception e) {
            LOGGER.error("Failed to create iceberg table.", logMap, Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
            throw e;
        }
        return table;
    }
}
