package com.apple.aml.stargate.common.http2;

import com.typesafe.config.Config;
import io.servicetalk.http.api.BlockingStreamingHttpService;
import io.servicetalk.http.api.StreamingHttpService;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.lang.invoke.MethodHandles;

import static com.apple.aml.stargate.common.utils.AppConfig.config;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;

@Component
@Lazy
public class Http2Server {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    @Lazy
    @Autowired(required = false)
    private StreamingHttpService asyncHandler;
    @Lazy
    @Autowired(required = false)
    private BlockingStreamingHttpService blockingHandler;

    @Async
    public void startServer() throws Exception {
        Config config = config();
        if ((!config.hasPath("http2.enabled") || config.getBoolean("http2.enabled")) && (asyncHandler != null || blockingHandler != null)) {
            int serverPort = config.getInt("http2.port");
            LOGGER.info("Starting http2 server at port {}", serverPort);
            Http2ServerUtils.startHttp2Server(asyncHandler, blockingHandler);
        } else {
            LOGGER.debug("Http2Server is disabled");
        }
    }
}
