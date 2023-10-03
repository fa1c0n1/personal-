package com.apple.aml.stargate.app.service;

import io.servicetalk.concurrent.api.Single;
import io.servicetalk.http.api.HttpServiceContext;
import io.servicetalk.http.api.StreamingHttpRequest;
import io.servicetalk.http.api.StreamingHttpResponse;
import io.servicetalk.http.api.StreamingHttpResponseFactory;
import io.servicetalk.http.api.StreamingHttpService;
import org.springframework.stereotype.Service;

import static io.servicetalk.concurrent.api.Single.succeeded;
import static io.servicetalk.http.api.HttpHeaderNames.ALLOW;
import static io.servicetalk.http.api.HttpRequestMethod.POST;

@Service
public class Http2StreamingService implements StreamingHttpService {
    @Override
    public Single<StreamingHttpResponse> handle(final HttpServiceContext ctx, final StreamingHttpRequest request, final StreamingHttpResponseFactory responseFactory) {
        if (!POST.equals(request.method())) {
            return succeeded(responseFactory.methodNotAllowed().addHeader(ALLOW, POST.name()));
        }
        return succeeded(responseFactory.ok());
    }
}
