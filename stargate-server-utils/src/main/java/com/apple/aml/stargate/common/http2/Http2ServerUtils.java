package com.apple.aml.stargate.common.http2;

import com.apple.aml.stargate.common.exceptions.MissingConfigException;
import com.typesafe.config.Config;
import io.servicetalk.http.api.BlockingStreamingHttpService;
import io.servicetalk.http.api.HttpServerBuilder;
import io.servicetalk.http.api.StreamingHttpService;
import io.servicetalk.http.netty.HttpServers;
import io.servicetalk.transport.api.ServerSslConfigBuilder;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.util.Map;

import static com.apple.aml.stargate.common.utils.AppConfig.config;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static io.servicetalk.http.netty.HttpProtocolConfigs.h2Default;

public final class Http2ServerUtils {

    private Http2ServerUtils() {
    }

    public static void startHttp2Server(final StreamingHttpService handler) throws Exception {
        startHttp2Server(handler, null);
    }

    public static void startHttp2Server(final StreamingHttpService asyncHandler, final BlockingStreamingHttpService blockingHandler) throws Exception {
        Config config = config();
        int port = config.getInt("http2.port");
        boolean sslEnabled = config.getBoolean("http2.server.ssl.enabled");
        HttpServerBuilder builder = HttpServers.forPort(port).protocols(h2Default());
        if (sslEnabled) {
            Path pemPath = Path.of(config.getString("http2.server.ssl.pem.path"));
            if (!pemPath.toFile().exists()) {
                throw new MissingConfigException("Missing config file", Map.of("fileName", pemPath));
            }
            Path keyPath = Path.of(config.getString("http2.server.ssl.key.path"));
            if (!keyPath.toFile().exists()) {
                throw new MissingConfigException("Missing config file", Map.of("fileName", keyPath));
            }
            String password = config.hasPath("http2.server.ssl.password") ? config.getString("http2.server.ssl.password") : null;
            if (isBlank(password)) {
                password = null;
            }

            ServerSslConfigBuilder sslBuilder = new ServerSslConfigBuilder(() -> {
                try {
                    return new FileInputStream(pemPath.toFile().getAbsolutePath());
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }, () -> {
                try {
                    return new FileInputStream(keyPath.toFile().getAbsolutePath());
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }, password);
            builder = builder.sslConfig(sslBuilder.build());
        }
        ("blocking".equalsIgnoreCase(config.getString("http2.server.type")) ? builder.listenBlockingStreamingAndAwait(blockingHandler) : builder.listenStreamingAndAwait(asyncHandler)).awaitShutdown();
    }

    public static void startHttp2Server(final BlockingStreamingHttpService handler) throws Exception {
        startHttp2Server(null, handler);
    }
}