package com.apple.aml.stargate.common.options;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.SIZE_CONVERSION_H_TO_BASE2;
import static com.apple.jvm.commons.util.Strings.isBlank;

@Data
public class ACIKafkaTopicOptions implements Serializable {
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private Integer partitionCount;
    private String producerQuota;
    private String consumerQuota;
    private String producerClientQuota;
    private String consumerClientQuota;
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private Integer retentionMilliSeconds;
    private String retentionBytes;

    public ACIKafkaTopicOptions() {

    }

    public ACIKafkaTopicOptions(final int partitionCount, final String producerQuota, final String consumerQuota, final String producerClientQuota, final String consumerClientQuota, final int retentionMilliSeconds, final String retentionBytes) {
        this.partitionCount = partitionCount;
        this.retentionMilliSeconds = retentionMilliSeconds;
        this.retentionBytes = retentionBytes;
        this.producerQuota = producerQuota;
        this.consumerQuota = consumerQuota;
        this.producerClientQuota = producerClientQuota;
        this.consumerClientQuota = consumerClientQuota;
    }

    public ACIKafkaTopicOptions(final double avgMsgSize, final String msgSizeUnit, final int msgCount, final String msgCountUnit, final Integer partitionCount, final int retentionMilliSeconds, final String retentionBytes) throws Exception {
        this.retentionMilliSeconds = retentionMilliSeconds;
        this.retentionBytes = retentionBytes;
        this.producerQuota = producerQuota(avgMsgSize, isBlank(msgSizeUnit) ? "byte" : msgSizeUnit, msgCount, msgCountUnit);
        //TODO:
        this.consumerQuota = this.producerQuota;
        this.producerClientQuota = this.producerQuota;
        this.consumerClientQuota = this.producerQuota;
        this.partitionCount = partitionCount;
    }

    public String producerQuota(final double avgMsgSize, final String msgSizeUnit, final int msgCount, final String msgCountUnit) throws Exception {
        String validSizeUnit;
        String key = msgSizeUnit.trim().substring(0, 1).toLowerCase();
        validSizeUnit = SIZE_CONVERSION_H_TO_BASE2.getOrDefault(key, "");
        double msgBytePerSec;
        switch (msgCountUnit.trim().toLowerCase().charAt(0)) {
            case 's':
                msgBytePerSec = msgCount * avgMsgSize;
                break;
            case 'h':
                msgBytePerSec = msgCount * avgMsgSize * 3600;
                break;
            case 'd':
                msgBytePerSec = msgCount * avgMsgSize * 86400;
                break;
            default:
                throw new Exception("Invaid msgCountUnit input");
        }
        return (int) Math.round(msgBytePerSec) + validSizeUnit;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> restPostBody(final String topic) {
        Map bandwith = Map.of("produce-bytes-per-sec", this.producerQuota, "consume-bytes-per-sec", this.producerQuota);
        Map entity;
        if (this.retentionMilliSeconds == null || this.retentionMilliSeconds == 0) {
            entity = Map.of("description", topic, "total-bandwidth", bandwith, "retention-bytes", this.retentionBytes, "partition-count", this.partitionCount == null || this.partitionCount < 1 ? 1 : this.partitionCount, "compacted", false);
        } else {
            entity = Map.of("description", topic, "total-bandwidth", bandwith, "retention-milliseconds", this.retentionMilliSeconds, "retention-bytes", this.retentionBytes, "partition-count", this.partitionCount == null || this.partitionCount < 1 ? 1 : this.partitionCount, "compacted", false);
        }
        Map requestBody = Map.of("name", topic, "entity", entity);
        return requestBody;
    }

    public void validate() throws Exception {
        if (isBlank(this.getProducerQuota())) {
            throw new Exception("Kafka Topic - Producer Quota ( producerQuota ) cannot be null or blank");
        }
        if (isBlank(this.getConsumerQuota())) {
            throw new Exception("Kafka Topic - Consumer Quota (consumerQuota) cannot be null or blank");
        }
        if (retentionMilliSeconds <= 0) {
            throw new Exception("Kafka Topic - Retention Duration ( retentionMilliSeconds ) should be a positive duration");
        }
    }
}
