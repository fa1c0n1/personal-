package com.apple.aml.stargate.app.pojo;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Date;

import static com.apple.jvm.commons.util.Strings.isBlank;

@Data
@EqualsAndHashCode(callSuper = true)
public class ModelPipeline extends ModelCleanupPipeline {
    private String appId;
    private String srcTopic;
    private boolean enableDirectAccess = true;
    private boolean enableEncryption;
    private Boolean autoCreateTopic = true;
    @ToString.Exclude
    private String targetPublicKey;
    private String targetUri;
    private String targetGroup;
    private String targetNamespace;
    private int targetPartitions = 1;
    private String targetTopic;
    private String targetClientId;
    @ToString.Exclude
    private String targetPrivateKey;
    private String callBackUrl;
    private Date initDate;
    private String latestActivityId;

    public Date initDate() {
        return this.initDate;
    }

    public String resolvedTargetPrivateKey() throws Exception {
        try {
            String decodedKey = new String(Base64.getDecoder().decode(targetPrivateKey), StandardCharsets.UTF_8);
            return decodedKey;
        } catch (Exception e) {
            throw new Exception("Invalid targetPrivateKey. Should be a valid Base64 encoded PrivateKey");
        }
    }

    public void validate() throws Exception {
        if (isBlank(this.getSrcTopic())) {
            throw new Exception("Source Topic cannot be null or blank");
        }
        if (enableDirectAccess) {
            if (!enableEncryption) {
                if (isBlank(this.targetPublicKey) || isBlank(this.resolvedTargetPublicKey())) {
                    throw new Exception("Invalid targetPublicKey");
                }
            }
        } else {
            if (isBlank(this.getTargetGroup())) {
                throw new Exception("Target Group cannot be null or blank");
            }
            if (isBlank(this.getTargetNamespace())) {
                throw new Exception("Target Namespace cannot be null or blank");
            }
            if (isBlank(this.getTargetTopic())) {
                throw new Exception("Target Topic cannot be null or blank");
            }
            if (isBlank(this.getTargetClientId())) {
                throw new Exception("Target ClientId cannot be null or blank");
            }
        }
    }

    public String resolvedTargetPublicKey() throws Exception {
        try {
            String decodedKey = new String(Base64.getDecoder().decode(targetPublicKey), StandardCharsets.UTF_8);
            return decodedKey;
        } catch (Exception e) {
            throw new Exception("Invalid targetPublicKey. Should be a valid Base64 encoded PublicKey");
        }
    }

}
