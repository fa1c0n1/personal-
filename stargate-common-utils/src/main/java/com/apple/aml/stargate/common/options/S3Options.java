package com.apple.aml.stargate.common.options;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import java.io.Serializable;
import java.util.regex.Pattern;

import static com.apple.aml.stargate.common.constants.CommonConstants.EMPTY_STRING;
import static com.apple.jvm.commons.util.Strings.isBlank;

@Data
@EqualsAndHashCode(callSuper = true)
public class S3Options extends HdfsOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Pattern BLOB_STORE_URL_PATTERN = Pattern.compile("(https|http):\\/\\/([0-9a-zA-Z-]+)\\.blobstore\\.apple\\.com");
    @Optional
    private String accessKey;
    @Optional
    private String secretKey;
    @Optional
    private String sessionToken;
    @Optional
    private String endpoint;
    @Optional
    private String region;
    @Optional
    private String s3Type;
    @Optional
    private String bucket;
    @Optional
    private String scheme;
    @Optional
    private boolean useHadoopBackend = true;

    @Override
    public String tmpPath(final String pipelineId) {
        if (isBlank(this.getTmpPath())) {
            return basePath();
        }
        return this.schemePath() + (this.getTmpPath().startsWith("/") ? this.getTmpPath() : ("/" + this.getTmpPath()));
    }

    private String schemePath() {
        return (isBlank(this.scheme) ? "s3a" : this.scheme.trim()) + "://" + bucket;
    }

    @Override
    public String basePath() {
        if (isBlank(this.getBasePath())) {
            return this.schemePath();
        }
        return this.schemePath() + (this.getBasePath().startsWith("/") ? this.getBasePath() : ("/" + this.getBasePath()));
    }

    public AmazonS3 client() {
        AmazonS3 client = AmazonS3ClientBuilder.standard().withCredentials(credentialsProvider()).withPathStyleAccessEnabled(pathStyle()).withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint(), region())).build();
        return client;
    }

    public AWSCredentialsProvider credentialsProvider() {
        return new AWSStaticCredentialsProvider(credentials());
    }

    public boolean pathStyle() {
        return s3Type == null || "aci".equalsIgnoreCase(s3Type);
    }

    public String endpoint() {
        if (endpoint.startsWith("http://") || endpoint.startsWith("https://")) {
            return endpoint;
        }
        return "http://store-" + endpoint.toLowerCase() + ".blobstore.apple.com";
    }

    public String region() {
        if (region == null || region.isBlank()) {
            String endpoint = endpoint();
            return endpoint.replace("https://", EMPTY_STRING).replace("http://", EMPTY_STRING).replace(".blobstore.apple.com", EMPTY_STRING); //return BLOB_STORE_URL_PATTERN.matcher(endpoint).group(2);
        }
        return region;
    }

    private AWSCredentials credentials() {
        return new BasicAWSCredentials(this.getAccessKey(), this.getSecretKey());
    }

    public AwsCredentialsProvider credsProvider() {
        return StaticCredentialsProvider.create(creds());
    }

    private AwsCredentials creds() {
        return AwsBasicCredentials.create(this.getAccessKey(), this.getSecretKey());
    }


}