package com.apple.aml.stargate.beam.sdk.io.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.apple.aml.stargate.beam.sdk.io.hadoop.HdfsIO;
import com.apple.aml.stargate.beam.sdk.utils.FileWriterFns;
import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.io.RawByteArrayOutputStream;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.S3Options;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.aws2.options.S3ClientBuilderFactory;
import org.apache.beam.sdk.io.aws2.s3.DefaultS3ClientBuilderFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.utils.AppConfig.config;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramBytes;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.histogramDuration;
import static com.apple.jvm.commons.util.Strings.isBlank;

public class S3IO extends HdfsIO {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final Pattern BUCKET_FILE_PATTERN = Pattern.compile("s3([an]?):\\/\\/([a-zA-Z0-9-_]+)\\/(.*)", Pattern.CASE_INSENSITIVE);

    @SuppressWarnings("unchecked")
    public static void init(final PipelineOptions pipelineOptions, final StargateNode node) throws Exception {
        LOGGER.debug("Setting up s3io writer configs - started");
        S3Options options = (S3Options) node.getConfig();
        if (options.isUseHadoopBackend()) {
            String bucket = options.getBucket(); // TODO : Should use nodeId instead
            Map<String, Object> coreProperties = options.getCoreProperties();
            if (coreProperties == null) {
                coreProperties = new HashMap<>();
                options.setCoreProperties(coreProperties);
            }
            if (!isBlank(options.getAccessKey())) coreProperties.put(String.format("fs.s3a.%s.access.key", bucket), options.getAccessKey());
            if (!isBlank(options.getSecretKey())) coreProperties.put(String.format("fs.s3a.%s.secret.key", bucket), options.getSecretKey());
            if (!isBlank(options.getSessionToken())) coreProperties.put(String.format("fs.s3a.%s.session.token", bucket), options.getSessionToken());
            if (!isBlank(options.getEndpoint())) coreProperties.put(String.format("fs.s3a.%s.endpoint", bucket), options.endpoint());
            if (!isBlank(options.getRegion())) coreProperties.put(String.format("fs.s3a.%s.region", bucket), options.region());
            if (options.pathStyle()) coreProperties.put(String.format("fs.s3a.%s.path.style.access", bucket), true);
            initWithDefaults(pipelineOptions, node, PipelineConstants.NODE_TYPE.S3.getConfig().getWriterDefaults());
        }
        LOGGER.debug("Setting up s3io writer configs - successful");
    }

    public static void uploadS3BulkStream(final String fileName, final List<RawByteArrayOutputStream> streams, final FileWriterFns.WriterKey writerKey) throws IOException, InterruptedException {
        int blockSize = config().getInt("stargate.s3.defaults.blockSize");
        Matcher matcher = BUCKET_FILE_PATTERN.matcher(fileName);
        if (!matcher.find()) {
            throw new IOException("Not a valid s3 path!! fileName : " + fileName);
        }
        String bucketName = matcher.group(2);
        String keyName = matcher.group(3);
        long totalStreamSize = streams.stream().map(s -> (long) s.size()).reduce((x, y) -> x + y).get();
        LOGGER.debug("Uploading streams via S3 Multipart upload", Map.of("fileName", fileName, "totalStreamSize", totalStreamSize, "noOfStreams", streams.size(), "bucketName", bucketName, "keyName", keyName), writerKey.logMap());
        AmazonS3 client = null;
        String uploadId = null;
        long startTime = System.nanoTime();
        try {
            int partNo = 1;
            List<Triple<Integer, InputStream, Integer>> parts = new ArrayList<>();
            long globalPosition = 0;
            int position = 0;
            int streamCounter = 0;
            RawByteArrayOutputStream stream = streams.get(streamCounter++);
            while (globalPosition < totalStreamSize) {
                byte[] bytes = stream.rawBytePointer();
                int partSize = (int) Math.min(blockSize, (totalStreamSize - globalPosition));
                globalPosition += partSize;
                if (position + partSize <= bytes.length) {
                    parts.add(Triple.of(partNo++, new ByteArrayInputStream(bytes, position, partSize), partSize));
                    position += partSize;
                    if (position == bytes.length) {
                        stream = streams.get(streamCounter++);
                        position = 0;
                    }
                    continue;
                }
                byte[] inputBytes = new byte[(int) partSize];
                int copySize = bytes.length - position;
                System.arraycopy(bytes, position, inputBytes, 0, copySize);
                stream = streams.get(streamCounter++);
                bytes = stream.rawBytePointer();
                position = partSize - copySize;
                System.arraycopy(bytes, 0, inputBytes, copySize, position);
            }
            client = AmazonS3ClientBuilder.standard().build();
            List<PartETag> partETags = new ArrayList<>();
            InitiateMultipartUploadResult initResponse = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, keyName));
            uploadId = initResponse.getUploadId();
            List<MultiPartFileUploader> uploaders = new ArrayList<>();
            AmazonS3 finalClient = client;
            String finalUploadId = uploadId;
            parts.stream().forEach(x -> {
                UploadPartRequest uploadRequest = new UploadPartRequest().withBucketName(bucketName).withKey(keyName).withUploadId(finalUploadId).withPartNumber(x.getLeft()).withInputStream(x.getMiddle()).withPartSize(x.getRight());
                MultiPartFileUploader uploader = new MultiPartFileUploader(finalClient, uploadRequest);
                uploaders.add(uploader);
                uploader.upload();
            });
            for (MultiPartFileUploader uploader : uploaders) {
                uploader.join();
                partETags.add(uploader.getPartETag());
            }
            CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(bucketName, keyName, uploadId, partETags);
            client.completeMultipartUpload(compRequest);
            histogramDuration(writerKey.getNodeName(), writerKey.getNodeType(), writerKey.getSchemaId(), "s3_multipart_upload_success").observe((System.nanoTime() - startTime) / 1000000.0);
            histogramBytes(writerKey.getNodeName(), writerKey.getNodeType(), writerKey.getSchemaId(), "stream_size_success").observe(totalStreamSize);
        } catch (Exception e) {
            histogramDuration(writerKey.getNodeName(), writerKey.getNodeType(), writerKey.getSchemaId(), "s3_multipart_upload_error").observe((System.nanoTime() - startTime) / 1000000.0);
            histogramBytes(writerKey.getNodeName(), writerKey.getNodeType(), writerKey.getSchemaId(), "stream_size_error").observe(totalStreamSize);
            try {
                if (uploadId != null && client != null) {
                    client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, keyName, uploadId));
                }
            } catch (Exception ex) {
                LOGGER.warn("Error aborting failed multipart request!!", Map.of(ERROR_MESSAGE, ex.getMessage(), "fileName", fileName), writerKey.logMap(), ex);
            }
            throw e;
        } finally {
            histogramDuration(writerKey.getNodeName(), writerKey.getNodeType(), writerKey.getSchemaId(), "s3_multipart_upload").observe((System.nanoTime() - startTime) / 1000000.0);
            histogramBytes(writerKey.getNodeName(), writerKey.getNodeType(), writerKey.getSchemaId(), "stream_size").observe(totalStreamSize);
        }
    }

    @SuppressWarnings("unchecked")
    public void initCommon(final Pipeline pipeline, final StargateNode node) throws Exception {
        setupS3Options(pipeline, (S3Options) node.getConfig());
        super.initCommon(pipeline, node);
    }

    public static void setupS3Options(final Pipeline pipeline, final S3Options options) {
        org.apache.beam.sdk.io.aws2.options.S3Options s3Options = pipeline.getOptions().as(org.apache.beam.sdk.io.aws2.options.S3Options.class);
        if (!isBlank(options.getAccessKey()) && !isBlank(options.getSecretKey())) s3Options.setAwsCredentialsProvider(options.credsProvider());
        if (!isBlank(options.getEndpoint())) s3Options.setEndpoint(URI.create(options.endpoint()));
        if (!isBlank(options.getRegion())) s3Options.setAwsRegion(Region.of(options.region()));
        s3Options.setS3ClientFactoryClass(s3ClientFactoryClass(options));
    }

    public static Class<? extends S3ClientBuilderFactory> s3ClientFactoryClass(S3Options options) {
        if (options.pathStyle()) {
            return PathStyleFactory.class;
        }
        return DefaultS3ClientBuilderFactory.class;
    }

    public SCollection<KV<String, GenericRecord>> write(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        return super.write(pipeline, node, collection);
    }

    public static class PathStyleFactory extends DefaultS3ClientBuilderFactory implements S3ClientBuilderFactory {
        @Override
        public S3ClientBuilder createBuilder(org.apache.beam.sdk.io.aws2.options.S3Options s3Options) {
            S3ClientBuilder builder = super.createBuilder(s3Options); // TODO : aws2 doesn't support it yet
            return builder;
        }
    }

    private static class MultiPartFileUploader extends Thread {
        private final AmazonS3 client;
        private final UploadPartRequest uploadRequest;
        private PartETag partETag;

        public MultiPartFileUploader(final AmazonS3 client, final UploadPartRequest uploadRequest) {
            this.client = client;
            this.uploadRequest = uploadRequest;
        }

        @Override
        public void run() {
            partETag = client.uploadPart(uploadRequest).getPartETag();
        }

        private PartETag getPartETag() {
            return partETag;
        }

        private void upload() {
            start();
        }

    }
}
