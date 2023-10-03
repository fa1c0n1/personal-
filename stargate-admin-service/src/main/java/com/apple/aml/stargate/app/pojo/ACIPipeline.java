package com.apple.aml.stargate.app.pojo;

import com.apple.aml.stargate.common.utils.EncryptionUtils;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.security.Key;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Date;

import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static java.nio.charset.StandardCharsets.UTF_8;

@Data
@EqualsAndHashCode(callSuper = true)
public class ACIPipeline extends ACICleanupPipeline {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private String appId;
    private String streamName;
    private String pipelineGroup;
    private String group;
    private String namespace;
    private double avgMsgSize;
    private String msgSizeUnit;  //accept byte, Ki, Mi, Gi, Ti, Pi
    private int msgCount;
    private String msgCountUnit; //accept sec, hr, day
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private int retentionMilliSeconds;
    private String retentionBytes;
    @Optional
    private Integer partitionCount;
    @ToString.Exclude
    //double encoded pem format
    private String privateKey;
    @ToString.Exclude
    //single encoded format
    private String publicKey;
    //Todo: setup levelKey
    private boolean levelKey;
    @Optional
    private String offset;
    @Optional
    private String callBackUrl;
    private Date initDate;
    private String latestActivityId;
    private String pipelineId;
    private String acUser;
    @ToString.Exclude
    private String acToken;

    public void generateKeyPairs() throws NoSuchAlgorithmException {
        KeyPair keyPair = EncryptionUtils.generateRSAKeyPairs(2048);
        Key publicKey = keyPair.getPublic();
        Key privateKey = keyPair.getPrivate();
        Base64.Encoder encoder = Base64.getEncoder();
        this.publicKey = encoder.encodeToString(publicKey.getEncoded());
        String singleEncodedPrivateKey = encoder.encodeToString(privateKey.getEncoded());
        this.privateKey = encoder.encodeToString(EncryptionUtils.toPEMPrivateKey(singleEncodedPrivateKey).getBytes(UTF_8));
    }

    public String pipelineId() {
        return this.getPipelineGroup() + "-" + this.getStreamName();
    }

    public Date initDate(final Date initDate) {
        this.initDate = initDate;
        return this.initDate;
    }

    public Date initDate() {
        return this.initDate;
    }

    public void validate() throws Exception {
        if (isBlank(this.getStreamName())) {
            throw new Exception("Stream name cannot be null or blank");
        }
        if (isBlank(this.getPipelineGroup())) {
            throw new Exception("PipelineGroup cannot be null or blank");
        }
        if (isBlank(this.group)) {
            throw new Exception("ACI Kafka Group cannot be null or blank");
        }
        if (isBlank(this.namespace)) {
            throw new Exception("ACI Kafka Namespace cannot be null or blank");
        }
        if (this.avgMsgSize <= 0) {
            throw new Exception("AvgMsgSize should be a positive number");
        }
        if (this.msgCount <= 0) {
            throw new Exception("MsgCount should be a positive number");
        }
        if (isBlank(this.getMsgCountUnit())) {
            throw new Exception("MsgCountUnit cannot be null or blank");
        }
        if (this.retentionMilliSeconds > 0 && this.retentionMilliSeconds < 1000) {
            throw new Exception("RetentionMilliSeconds is too small got: " + this.retentionMilliSeconds + " < 1000");
        }
    }

    public String appId(final String appId) {
        this.appId = appId;
        return this.appId;
    }

    public String appId() {
        return this.appId;
    }

    public String latestActivityId(final String latestActivityId) {
        this.latestActivityId = latestActivityId;
        return this.latestActivityId;
    }

    public String latestActivityId() {
        return this.latestActivityId;
    }

}

