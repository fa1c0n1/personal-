package com.apple.aml.stargate.connector.stratos;


import com.apple.aml.stargate.common.constants.A3Constants;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.apple.aml.stargate.common.options.StratosOptions;
import com.apple.aml.stargate.common.utils.AppConfig;
import com.apple.aml.stargate.common.utils.JsonUtils;
import com.apple.ist.ets.geneva.APIConfig;
import com.apple.ist.ets.geneva.Credentials;
import com.apple.ist.ets.geneva.Geneva;
import com.apple.ist.ets.geneva.SSLContextCreator;
import com.apple.ist.ets.geneva.exception.AuthorizationException;
import com.apple.ist.ets.geneva.exception.BadRequestException;
import com.apple.ist.ets.geneva.exception.InvalidArgumentException;
import com.apple.ist.ets.geneva.exception.TopicNotFoundException;
import com.apple.ist.ets.geneva.exception.UnsupportedGenevaOperationException;
import com.apple.ist.ets.geneva.services.QueueService;
import com.apple.ist.ets.geneva.services.vo.AckResponseObject;
import com.apple.ist.ets.geneva.services.vo.AckWithReceiptRequestObject;
import com.apple.ist.ets.geneva.services.vo.queue.GetQueueRequestObject;
import com.apple.ist.ets.geneva.services.vo.queue.GetQueueResponseObject;
import com.apple.ist.ets.geneva.services.vo.queue.QueueReceipt;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;
import org.slf4j.Logger;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

import static com.apple.aml.stargate.common.constants.CommonConstants.FreemarkerNames.KEY;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NODE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.UNKNOWN;
import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter.converter;
import static com.apple.aml.stargate.common.utils.A3Utils.getA3Token;
import static com.apple.aml.stargate.common.utils.A3Utils.getCachedToken;
import static com.apple.aml.stargate.common.utils.ClassUtils.parseBoolean;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.fetchSchemaWithLocalFallback;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static java.nio.charset.StandardCharsets.UTF_8;

public class StratosReader extends UnboundedSource.UnboundedReader<KV<String, GenericRecord>> {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final long STRATOS_APP_ID = A3Constants.KNOWN_APP.STRATOS.appId();
    private final StratosSource source;
    private final String nodeName;
    private final String nodeType;
    private final StratosOptions options;
    private final String sharedDirectory;
    private Geneva geneva;
    private QueueService queueService;
    private boolean a3Enabled;
    private final PipelineConstants.ENVIRONMENT a3Environment;
    private String fullServiceName;
    private String stratosContext;
    private byte[] stratosContextBytes;
    private boolean autoAcknowledge;
    private Schema schema;
    private ConcurrentHashMap<String, ObjectToGenericRecordConverter> converterMap = new ConcurrentHashMap<>();
    private ResponseHolder current = null;

    public StratosReader(final StratosSource source, final String nodeName, final String nodeType, final StratosOptions options, final String sharedDirectory) {
        this.source = source;
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        this.options = options;
        this.sharedDirectory = sharedDirectory;
        this.a3Enabled = isBlank(options.getAuthType()) || !options.getAuthType().toLowerCase().startsWith("cert");
        this.a3Environment = isBlank(this.options.getA3Mode()) ? AppConfig.environment() : PipelineConstants.ENVIRONMENT.environment(this.options.getA3Mode());
        this.autoAcknowledge = parseBoolean(this.options.getAutoAcknowledge());
        if (!isBlank(this.options.getSchemaId())) {
            this.schema = fetchSchemaWithLocalFallback(options.getSchemaReference(), options.getSchemaId());
            this.converterMap.put(this.options.getSchemaId(), converter(schema, options));
            LOGGER.debug("Stratos schema converter created successfully for", Map.of(SCHEMA_ID, options.getSchemaId(), NODE_NAME, nodeName));
        }
    }

    private SSLContextCreator sslContextCreator() {
        return () -> {
            try {
                return SSLContext.getDefault();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    public boolean start() throws IOException {
        try {
            APIConfig config = JsonUtils.merge(new APIConfig(APIConfig.GenevaEnv.fromStringEnv(options.genevaEnvironment())), jsonString(options));
            String appEnv = String.format("%s.%s", options.getAppId(), options.serviceEnvironment());
            if (a3Enabled) {
                this.geneva = new Geneva(sslContextCreator(), appEnv, config);
            } else {
                Path propsFile = Paths.get(String.format("%s/certs", sharedDirectory), String.format("%s.pkcs", nodeName));
                if (!propsFile.toFile().exists()) {
                    propsFile.getParent().toFile().mkdirs();
                    Files.write(propsFile, Base64.getDecoder().decode(options.getCert()));
                }
                this.geneva = new Geneva(new Credentials(propsFile.toFile().getAbsolutePath(), options.getCertPassword()), appEnv, config);
            }
            this.queueService = geneva.queueService(options.getTopic());
            this.fullServiceName = String.format("#%s.%s#", options.getAppId(), options.getTopic());
            this.stratosContext = String.format("%s|%s", options.consumerAppName(), options.context());
            this.stratosContextBytes = this.stratosContext.getBytes(UTF_8);
        } catch (Exception e) {
            LOGGER.error("Error in starting stratos reader", Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType, ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
            throw new IOException(e);
        }
        return false;
    }

    @Override
    public boolean advance() throws IOException {
        String transactionId = UNKNOWN;
        int statusCode = -1;
        String receiptId = UNKNOWN;
        try {
            GetQueueRequestObject request = new GetQueueRequestObject();
            request.setSubscriber(stratosContextBytes);
            if (a3Enabled) {
                String a3Token = isBlank(options.getA3Password()) ? getA3Token(STRATOS_APP_ID, fullServiceName, 3L) : getCachedToken(options.getA3AppId(), STRATOS_APP_ID, options.getA3Password(), fullServiceName, 3L, a3Environment);
                request.addCustomHeader("x-geneva-auth-token-type", "A3_TOKEN");
                request.addCustomHeader("x-geneva-auth-token", a3Token);
                request.addCustomHeader("x-geneva-subscriber", stratosContext);
            }
            GetQueueResponseObject response;
            try {
                response = queueService.get(request);
                if (response == null) return false;
            } catch (IOException ex) {
                // Stratos api is responding with IOException with message as Bad Request when no new record is available; This is Stratos api's limitation
                if (LOGGER.isTraceEnabled()) LOGGER.trace("Error in fetching new stratos record. Will assume no new record", Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType, ERROR_MESSAGE, String.valueOf(ex.getMessage()), "statusCode", statusCode, "transactionId", transactionId, "receiptId", receiptId), ex);
                return false;
            }
            statusCode = response.getStatusCode();
            transactionId = response.getTransactionId();
            receiptId = new String(response.getReceipt().getReceiptId());
            this.current = ResponseHolder.builder().reader(this).value(response).acked(autoAcknowledge).build();
            return true;
        } catch (InvalidArgumentException | TopicNotFoundException | UnsupportedGenevaOperationException | BadRequestException | AuthorizationException e) {
            LOGGER.error("Error in checking for new stratos record", Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType, ERROR_MESSAGE, String.valueOf(e.getMessage()), "statusCode", statusCode, "transactionId", transactionId, "receiptId", receiptId), e);
            throw new IOException(e);
        } catch (Exception e) {
            LOGGER.warn("Error in checking for new stratos record. Will retry again later", Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType, ERROR_MESSAGE, String.valueOf(e.getMessage()), "statusCode", statusCode, "transactionId", transactionId, "receiptId", receiptId), e);
            return false;
        }
    }

    @Override
    public Instant getWatermark() {
        return Instant.now();
    }

    @Override
    public UnboundedSource.CheckpointMark getCheckpointMark() {
        return new StratosCheckMark();
    }

    @Override
    public UnboundedSource<KV<String, GenericRecord>, ?> getCurrentSource() {
        return source;
    }

    @Override
    public KV<String, GenericRecord> getCurrent() throws NoSuchElementException {
        if (current == null) throw new NoSuchElementException();
        String uuid = UNKNOWN;
        String schemaId = UNKNOWN;
        String transactionId = UNKNOWN;
        String receiptId = UNKNOWN;
        try {
            QueueReceipt receipt = current.getValue().getReceipt();
            uuid = new String(receipt.getUUID());
            transactionId = receipt.getTransactionId();
            receiptId = new String(receipt.getReceiptId());
            Map<Object, Object> responsePayload = readJsonMap(new String(receipt.getBytes()));
            Map<Object, Object> responseMetadata = readJsonMap(new String(receipt.getMetadata()));
            Schema recordSchema = responseMetadata.containsKey("schemaId") ? fetchSchemaWithLocalFallback(options.getSchemaReference(), responseMetadata.get("schemaId").toString()) : schema;
            if (recordSchema == null) throw new IOException("Could not read/derive schema for stratos record!!");
            schemaId = recordSchema.getFullName();
            GenericRecord record = this.converterMap.computeIfAbsent(schemaId, s -> converter(recordSchema)).convert(responsePayload);
            current.ack();
            return KV.of(uuid, record);
        } catch (Exception e) {
            LOGGER.error("Error in reading/converting stratos record to generic record!!", Map.of(NODE_NAME, nodeName, NODE_TYPE, nodeType, ERROR_MESSAGE, String.valueOf(e.getMessage()), KEY, uuid, SCHEMA_ID, schemaId, "transactionId", transactionId, "receiptId", receiptId), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] getCurrentRecordId() throws NoSuchElementException {
        if (current == null) throw new NoSuchElementException();
        return current.getValue().getReceipt().getUUID();
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
        if (current == null) throw new NoSuchElementException();
        return Instant.ofEpochMilli(this.current.getValue().getPublishTime());
    }

    @Override
    public void close() throws IOException {
        if (geneva != null) geneva.close();
    }

    @Data
    @Builder
    public static class ResponseHolder {
        private GetQueueResponseObject value;
        private StratosReader reader;
        private boolean acked;

        @SneakyThrows
        public ResponseHolder ack() {
            if (acked) return this;
            AckWithReceiptRequestObject ackRequest = new AckWithReceiptRequestObject();
            if (reader.a3Enabled) {
                String a3Token = isBlank(reader.options.getA3Password()) ? getA3Token(STRATOS_APP_ID, reader.fullServiceName, 3L) : getCachedToken(reader.options.getA3AppId(), STRATOS_APP_ID, reader.options.getA3Password(), reader.fullServiceName, 3L, reader.a3Environment);
                ackRequest.addCustomHeader("x-geneva-auth-token-type", "A3_TOKEN");
                ackRequest.addCustomHeader("x-geneva-auth-token", a3Token);
                ackRequest.addCustomHeader("x-geneva-subscriber", reader.stratosContext);
            }
            final AckResponseObject ackResponse = this.value.getReceipt().acknowledge(ackRequest);
            this.acked = ackResponse != null && ackResponse.getStatusCode() == 200;
            return this;
        }
    }
}
