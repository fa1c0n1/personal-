package com.apple.aml.stargate.beam.sdk.io.solr;

import com.apple.aml.stargate.beam.sdk.options.StargateOptions;
import com.apple.aml.stargate.beam.sdk.triggers.Sequencer;
import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.SolrOptions;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.solr.SolrIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.Krb5HttpClientBuilder;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.joda.time.Duration;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static com.apple.aml.stargate.beam.sdk.triggers.Sequencer.getSchema;
import static com.apple.aml.stargate.beam.sdk.utils.WindowFns.applyWindow;
import static com.apple.aml.stargate.beam.sdk.utils.WindowFns.batchPartitionKey;
import static com.apple.aml.stargate.beam.sdk.utils.WindowFns.batchWriter;
import static com.apple.aml.stargate.beam.sdk.values.SCollection.GENERIC_RECORD_GROUP_TAG;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_CONF;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_KEYTAB;
import static com.apple.aml.stargate.common.utils.FormatUtils.parseDateTime;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.getKrb5FileContent;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.setSolrKrb5SystemProps;
import static com.apple.jvm.commons.util.Strings.isBlank;

public final class GenericSolrIO {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    public static SolrClient solrClient(final SolrOptions options) {
        if (options.isUseZookeeper()) {
            CloseableHttpClient httpClient;
            if (options.isKerberizationEnabled()) {
                HttpClientUtil.setHttpClientBuilder(new Krb5HttpClientBuilder().getBuilder());
                httpClient = HttpClientUtil.createClient(new ModifiableSolrParams(), getHttpClientConnectionManager(options));
            } else if (options.isUseBasicAuth()) {
                httpClient = getHttpClient(options);
            } else {
                httpClient = HttpClients.custom().setConnectionManager(getHttpClientConnectionManager(options)).build();
            }
            return cloudSolrClient(options, httpClient);
        }
        if (options.isUseBasicAuth()) return httpSolrClient(options, getHttpClient(options));

        return httpSolrClient(options);
    }

    private static PoolingHttpClientConnectionManager getHttpClientConnectionManager(final SolrOptions options) {
        PoolingHttpClientConnectionManager poolingClientCM = new PoolingHttpClientConnectionManager();
        poolingClientCM.setMaxTotal(options.getMaxConnections());
        poolingClientCM.setDefaultMaxPerRoute(options.getMaxConnectionPerRoute());

        return poolingClientCM;
    }

    private static CloseableHttpClient getHttpClient(final SolrOptions options) {
        CredentialsProvider provider = new BasicCredentialsProvider();
        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(options.getUser(), options.getPassword());
        provider.setCredentials(AuthScope.ANY, credentials);

        return HttpClientBuilder.create().setDefaultCredentialsProvider(provider).setConnectionManager(getHttpClientConnectionManager(options)).build();
    }

    private static SolrClient cloudSolrClient(final SolrOptions options, final CloseableHttpClient httpClient) {
        CloudSolrClient.Builder builder = new CloudSolrClient.Builder(options.getZookeeperHostList(), Optional.of(options.getZkChroot())).withHttpClient(httpClient);
        if (options.getConnectionTimeout() != null) {
            builder = builder.withConnectionTimeout((int) options.getConnectionTimeout().toMillis());
        }
        if (options.getSocketTimeout() != null) {
            builder = builder.withSocketTimeout((int) options.getSocketTimeout().toMillis());
        }
        return builder.build();
    }

    private static SolrClient httpSolrClient(final SolrOptions options, final CloseableHttpClient httpClient) {
        HttpSolrClient.Builder builder = new HttpSolrClient.Builder(options.getUrl()).withHttpClient(httpClient);
        return setTimeout(options, builder);
    }

    private static SolrClient httpSolrClient(final SolrOptions options) {
        HttpSolrClient.Builder builder = new HttpSolrClient.Builder(options.getUrl());
        return setTimeout(options, builder);
    }

    private static SolrClient setTimeout(final SolrOptions options, HttpSolrClient.Builder builder) {
        if (options.getConnectionTimeout() != null) {
            builder = builder.withConnectionTimeout((int) options.getConnectionTimeout().toMillis());
        }
        if (options.getSocketTimeout() != null) {
            builder = builder.withSocketTimeout((int) options.getSocketTimeout().toMillis());
        }
        return builder.build();
    }

    public static void init(final PipelineOptions pOptions, final StargateNode node) throws Exception {
        SolrOptions options = (SolrOptions) node.getConfig();
        options.sanitizeAuthOptions();
        options.enableSchemaLevelDefaults();
        if (options.isKerberizationEnabled()) krb5AuthSetup(pOptions, node, options);
    }

    private static void krb5AuthSetup(final PipelineOptions pOptions, final StargateNode node, final SolrOptions options) throws Exception {
        StargateOptions pipelineOptions = pOptions.as(StargateOptions.class);
        String sharedDir = pipelineOptions.getSharedDirectoryPath();
        String keytabFilePath = String.format("%s/" + options.keytabFileName() + STARGATE_KEYTAB, sharedDir);
        String krb5FilePath = String.format("%s/" + options.krb5FileName() + STARGATE_CONF, sharedDir);
        String jaasFilePath = String.format("%s/" + options.jaasFileName() + STARGATE_CONF, sharedDir);
        boolean dryRun = pipelineOptions.isDryRun();
        if (!dryRun) {
            Files.write(Paths.get(krb5FilePath), getKrb5FileContent(node, options.getKrb5(), options.krb5FileName(), sharedDir, keytabFilePath));
            Files.write(Paths.get(keytabFilePath), (byte[]) node.configFiles().get(options.keytabFileName()));
            Files.write(Paths.get(jaasFilePath), (byte[]) node.configFiles().get(options.jaasFileName()));
            setSolrKrb5SystemProps(options.jaasFileName() + STARGATE_CONF, options.krb5FileName() + STARGATE_CONF, options.isUseKrb5AuthDebug());
        }
    }

    public SCollection<KV<String, GenericRecord>> read(final Pipeline pipeline, final StargateNode node) throws Exception {
        SolrOptions solrOptions = (SolrOptions) node.getConfig();
        solrOptions.setDynamic(false);
        if (solrOptions.getOptions() == null || solrOptions.getOptions().isEmpty()) {
            if (solrOptions.isUseInbuilt() && !(isBlank(solrOptions.getZkHost()))) {
                SolrIO.ConnectionConfiguration config = SolrIO.ConnectionConfiguration.create(solrOptions.getZkHost()).withBasicCredentials(solrOptions.getUser(), solrOptions.getPassword());
                SolrIO.Read reader = SolrIO.read().from(solrOptions.getCollection()).withQuery(solrOptions.getQuery()).withConnectionConfiguration(config);
                return SCollection.apply(pipeline, node.name("read"), reader).apply(node.name("convert"), new InbuiltSolrTransformer(node.name("convert"), node.getType(), node.environment(), solrOptions));
            } else {
                StargateOptions sOptions = pipeline.getOptions().as(StargateOptions.class);
                Instant pipelineInvokeTime = parseDateTime(sOptions.getPipelineInvokeTime());
                PCollection<KV<String, GenericRecord>> collection;
                if (solrOptions.isBounded()) {
                    SolrSDFReader reader = new SolrSDFBoundedReader(node.name("read"), node.getType(), node.environment(), pipelineInvokeTime, solrOptions);
                    collection = pipeline.apply(node.name("root"), Create.of(pipelineInvokeTime.toEpochMilli())).apply(node.name("read"), ParDo.of(reader));
                } else {
                    SolrSource reader = new SolrSource(node.name("unbounded-reader"), node.getType(), node.environment(), solrOptions, pipeline.getCoderRegistry().getCoder(Long.class), pipelineInvokeTime);
                    collection = pipeline.apply(node.name("unbounded-reader"), org.apache.beam.sdk.io.Read.from(reader)).apply(ParDo.of(new SolrBatchProcessor(node.name("solr-batch-reader"), node.getType(), node.environment(), pipelineInvokeTime, solrOptions)));
                }
                return SCollection.of(pipeline, collection);
            }
        } else {
            SCollection<KV<String, GenericRecord>> collection = SCollection.apply(pipeline, node.name("root"), GenerateSequence.from(0).to(1).withRate(1, Duration.standardSeconds(1))).apply(node.name("add-key"), new Sequencer.Operate(node.getName(), node.getType(), getSchema(node.environment())));
            return invoke(node, solrOptions, true, collection);
        }
    }

    private SCollection<KV<String, GenericRecord>> invoke(final StargateNode node, final SolrOptions options, final boolean emit, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        if (options.getOperation() == null || "read".equalsIgnoreCase(options.getOperation())) {
            return collection.apply(node.getName(), new SolrReader(node.getName(), node.getType(), node.environment(), options));
        }
        SCollection<KV<String, GenericRecord>> window = applyWindow(collection, options, node.getName());
        if (options.getBatchSize() > 1) {
            LOGGER.debug("Optimized batching enabled. Will apply batching", Map.of("batchSize", options.getBatchSize(), "partitions", options.getPartitions()));
            return window.group(node.name("partition-key"), batchPartitionKey(options.getPartitions(), options.getPartitionStrategy()), GENERIC_RECORD_GROUP_TAG).apply(node.name("batch"), batchWriter(node.getName(), node.getType(), node.environment(), options, new SolrWriter<>(node.getName(), node.getType(), node.environment(), options, emit)));
        } else {
            return window.apply(node.getName(), new SolrRecordWriter(node.getName(), node.getType(), node.environment(), options, emit));
        }
    }

    public SCollection<KV<String, GenericRecord>> transform(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        return invoke(node, (SolrOptions) node.getConfig(), true, collection);
    }

    @SuppressWarnings("unchecked")
    public SCollection<KV<String, GenericRecord>> write(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        SolrOptions options = (SolrOptions) node.getConfig();
        options.setOperation("write");
        return invoke(node, (SolrOptions) node.getConfig(), false, collection);
    }
}
