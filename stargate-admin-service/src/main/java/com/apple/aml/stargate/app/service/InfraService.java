package com.apple.aml.stargate.app.service;

import com.apple.aml.stargate.app.config.AppDefaults;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Service;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static java.lang.String.join;

@Service
public class InfraService {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    @Autowired
    private AppDefaults appDefaults;

    public Map<String, Object> logHeaders(final ServerHttpRequest request) {
        if (request == null || appDefaults.getLogHeadersMapping().isEmpty()) {
            return null;
        }
        Map<String, Object> logHeaders = new HashMap<>(appDefaults.getLogHeadersMapping().size());
        HttpHeaders httpHeaders = request.getHeaders();
        for (Map.Entry<String, String> entry : appDefaults.getLogHeadersMapping().entrySet()) {
            try {
                List<String> value = httpHeaders.get(entry.getKey());
                if (value == null) {
                    continue;
                }
                logHeaders.put(entry.getValue(), join(",", value));
            } catch (Exception e) {
                logHeaders.put(entry.getValue(), "Could not collect header value. Error : " + e.getMessage());
            }
        }
        return logHeaders;
    }
}
