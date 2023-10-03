package com.apple.aml.stargate.beam.sdk.fs.s3a;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.S3ClientOptions;
import com.apple.aml.stargate.common.constants.CommonConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.DefaultS3ClientFactory;

import java.io.IOException;
import java.net.URI;

import static com.apple.jvm.commons.util.Strings.isBlank;
import static org.apache.hadoop.fs.s3a.Constants.ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_BUCKET_PREFIX;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_PREFIX;
import static org.apache.hadoop.fs.s3a.Constants.PATH_STYLE_ACCESS;

public class BucketSpecificS3ClientFactory extends DefaultS3ClientFactory {
    private static final String BUCKET_PATTERN = FS_S3A_BUCKET_PREFIX + "%s.%s";

    @Override
    public AmazonS3 createS3Client(final URI uri, final S3ClientCreationParameters parameters) throws IOException {
        return configureBucketSpecificAmazonS3Client(super.createS3Client(uri, parameters), uri.getHost(), getConf());
    }

    private static AmazonS3 configureBucketSpecificAmazonS3Client(final AmazonS3 s3, final String bucket, final Configuration conf) throws IllegalArgumentException {
        String subkey = ENDPOINT.substring(FS_S3A_PREFIX.length());
        String shortBucketKey = String.format(BUCKET_PATTERN, bucket, subkey);
        String endPoint = conf.getTrimmed(shortBucketKey, "");
        if (isBlank(endPoint)) endPoint = conf.getTrimmed(ENDPOINT, CommonConstants.EMPTY_STRING);
        if (!isBlank(endPoint)) {
            try {
                s3.setEndpoint(endPoint);
            } catch (IllegalArgumentException e) {
                String msg = "Incorrect endpoint: " + e.getMessage();
                LOG.error(msg);
                throw new IllegalArgumentException(msg, e);
            }
        }
        return applyS3ClientOptions(s3, bucket, conf);
    }

    private static AmazonS3 applyS3ClientOptions(final AmazonS3 s3, final String bucket, final Configuration conf) {
        String subkey = PATH_STYLE_ACCESS.substring(FS_S3A_PREFIX.length());
        String shortBucketKey = String.format(BUCKET_PATTERN, bucket, subkey);
        final boolean pathStyleAccess = conf.getBoolean(shortBucketKey, false) || conf.getBoolean(PATH_STYLE_ACCESS, false);
        if (pathStyleAccess) {
            LOG.debug("Enabling path style access!");
            s3.setS3ClientOptions(S3ClientOptions.builder().setPathStyleAccess(true).build());
        }
        return s3;
    }
}
