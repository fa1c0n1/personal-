package com.apple.aml.stargate.common.utils;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3;

import static com.apple.aml.stargate.common.utils.ClassUtils.fetchClassIfExists;

public final class S3Utils {
    private static final boolean builderClassExists = fetchClassIfExists("com.amazonaws.services.s3.AmazonS3ClientBuilder") != null;

    private S3Utils() {

    }

    @SuppressWarnings("deprecation")
    public static AmazonS3 client(final AWSCredentials credentials, final boolean pathStyle, final String endpoint, final String region) {
        if (builderClassExists) {
            AmazonS3 client = com.amazonaws.services.s3.AmazonS3ClientBuilder.standard()
                    .withCredentials(new com.amazonaws.auth.AWSStaticCredentialsProvider(credentials))
                    .withPathStyleAccessEnabled(pathStyle)
                    .withEndpointConfiguration(new com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration(endpoint, region))
                    .build();
            return client;
        }
        ClientConfiguration config = new ClientConfiguration();
        AmazonS3 client = new com.amazonaws.services.s3.AmazonS3Client(credentials, config);
        client.setEndpoint(endpoint);
        return client;
    }
}
