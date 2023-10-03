package com.apple.aml.stargate.common.options;

import com.apple.aml.stargate.common.annotation.DesignerSpec;
import com.apple.aml.stargate.common.annotation.DesignerSpec.Type;
import com.apple.aml.stargate.common.constants.PipelineConstants.DATA_FORMAT;
import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.common.constants.CommonConstants.DEFAULT_DELIMITER;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static com.google.common.primitives.Ints.asList;

@Data
@EqualsAndHashCode(callSuper = true)
public class ACIKafkaOptions extends BaseOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    @DesignerSpec(label = "Uri", toolTip = "Kaffe Url", type = Type.SingleLineText)
    private String uri;
    @Optional
    @DesignerSpec(label = "Api Uri", toolTip = "Kaffe Web API Url", type = Type.SingleLineText)
    private String apiUri;
    @Optional
    @DesignerSpec(label = "Api Token", toolTip = "Kaffe Web API Access Token", type = Type.SingleLineText)
    private String apiToken;
    @Optional
    @DesignerSpec(label = "Bootstrap Servers", toolTip = "Kafka Bootstrap Servers", type = Type.SingleLineText)
    private String bootstrapServers;
    @Optional
    @DesignerSpec(label = "Group", toolTip = "Group", type = Type.SingleLineText)
    private String group;
    @Optional
    @DesignerSpec(label = "Namespace", toolTip = "Kaffe Namespace", type = Type.SingleLineText)
    private String namespace;
    @Optional
    @DesignerSpec(label = "Topic", toolTip = "Topic", type = Type.SingleLineText)
    private String topic;
    @Optional
    @DesignerSpec(label = "Partition", toolTip = "Partition", type = Type.Number, min = 0, max = 500)
    private Integer partition;
    @Optional
    @DesignerSpec(label = "Partitions", toolTip = "Partitions", type = Type.List, min = 0, max = 500)
    private List<Integer> partitions;
    @Optional
    @DesignerSpec(label = "PartitionsCsv", toolTip = "PartitionsCsv", type = Type.SingleLineText)
    private String partitionsCsv;
    @Optional
    @DesignerSpec(label = "Total Partitions", toolTip = "Total Partitions", type = Type.Number, min = 1, max = 500)
    private Integer totalPartitions;
    @Optional
    @DesignerSpec(label = "Topics", toolTip = "Topics", type = Type.List)
    private List<String> topics;
    @Optional
    @DesignerSpec(label = "ClientID", toolTip = "ClientID", type = Type.SingleLineText)
    private String clientId;
    @Optional
    @DesignerSpec(label = "Consumer GroupID", toolTip = "Consumer GroupID", type = Type.SingleLineText)
    private String consumerGroupId;
    @Optional
    @DesignerSpec(label = "Producer ID", toolTip = "Producer ID", type = Type.SingleLineText)
    private String producerId;
    @Optional
    @DesignerSpec(label = "Private Key", toolTip = "Client Private Key", type = Type.MultiLineSecret)
    private String privateKey;
    @Optional
    @DesignerSpec(label = "Offset", toolTip = "Offset", type = Type.SingleChoice, choices = {"latest", "earliest", "none"})
    private String offset;
    @Optional
    @DesignerSpec(label = "Props", toolTip = "Other Kafka Properties", type = Type.MultiLineText)
    private Map<String, Object> props;
    @Optional
    @DesignerSpec(label = "Format", toolTip = "Format", type = Type.SingleChoice, choices = {"AVRO", "JSON", "ByteArray", "ByteArrayString"})
    private String format;
    @Optional
    @DesignerSpec(label = "Max No of Records", toolTip = "Max No of Records", type = Type.Number)
    private Long maxNumRecords;
    @Optional
    @DesignerSpec(label = "Source Topic", toolTip = "Source Topic", type = Type.MultiLineText)
    private transient ACIKafkaTopicOptions sourceTopic;
    @Optional
    @DesignerSpec(label = "Timestamp Policy", toolTip = "Timestamp Policy", type = Type.SingleChoice, choices = {"Processing Time", "Log Append Time", "Create Time"})
    private String timeStampPolicy;
    @Optional
    @DesignerSpec(label = "Auto Commit", toolTip = "Auto Commit", type = Type.YesNo)
    private boolean autoCommit;
    @Optional
    @DesignerSpec(label = "Vanilla", toolTip = "Vanilla", type = Type.YesNo)
    private boolean vanilla;
    @Optional
    @DesignerSpec(label = "Read Start Time", toolTip = "Read Start Time", type = Type.SingleLineText)
    private String readFrom;
    @Optional
    @DesignerSpec(label = "Read End Time", toolTip = "Read End Time", type = Type.SingleLineText)
    private String readTill;
    @Optional
    private boolean emitMetadata;
    @Optional
    private boolean includeMetadata;
    @Optional
    private String sql;
    @Optional
    private boolean randomPayloadKey = true;
    @Optional
    private Map<String, String> schemaIdTopicMappings;
    @Optional
    private String topicExpression;
    @Optional
    private boolean fullyQualifiedTopicName;
    @Optional
    private boolean fullyQualifiedClientId;
    @Optional
    private boolean logToForwarder;
    @Optional
    private boolean decryptionEnabled;
    @Optional
    private boolean encryptionEnabled;
    @Optional
    private boolean cryptoEnabled;
    @Optional
    @DesignerSpec(label = "kafkaDevMode", toolTip = "Kafka Dev mode", type = Type.YesNo)
    private boolean kafkaDevMode;
    @Optional
    private boolean useRawMessage;
    @Optional
    private int partitionGroups;
    @Optional
    private boolean useReaderSpecificConsumer;
    @Optional
    private Duration partitionWatchDuration;
    @Optional
    private boolean useInbuilt = true;
    @Optional
    private Duration pollTimeout = Duration.ofSeconds(1);
    @Optional
    private Duration pollRetryDuration = Duration.ofSeconds(5);
    @Optional
    private boolean enableKafkaMetrics;
    @Optional
    private int maxPollReset = 60;
    @Optional
    private int minInMemoryRecords = 10; // per partition; applicable if not using poller/on-fly polling
    @Optional
    private int maxInMemoryRecords = -1; // per partition; applicable if using pollers
    @Optional
    private int pollers = 0; // per partition
    @Optional
    private int maxTps = 0; // per partition
    @Optional
    private boolean shuffle;
    @Optional
    private boolean commitWithoutCheckpoint;
    @Optional
    private Duration commitInterval = Duration.ofSeconds(5);
    @Optional
    private boolean appendOffsetToKey = true;
    @Optional
    private boolean enableLatestOffsetPoller = true;
    @Optional
    private long maxPollRecords;
    @Optional
    private String compression;

    public DATA_FORMAT dataFormat() {
        if (this.getFormat() == null) {
            return DATA_FORMAT.avro;
        }
        return DATA_FORMAT.valueOf(this.getFormat().trim().toLowerCase());
    }

    public boolean decryptionFlag() {
        return decryptionEnabled || cryptoEnabled;
    }

    public boolean encryptionFlag() {
        return encryptionEnabled || cryptoEnabled;
    }

    public List<Integer> partitions() {
        if (this.partitions != null) return partitions;
        if (this.partition != null && this.partition >= 0) return asList(this.partition);
        if (!isBlank(this.partitionsCsv)) return Arrays.stream(this.partitionsCsv.trim().split(DEFAULT_DELIMITER)).map(x -> Integer.parseInt(x)).collect(Collectors.toList());
        return null;
    }
}
