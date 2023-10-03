package com.apple.aml.stargate.common.grpc.handlers;

import com.apple.aml.stargate.common.spring.handlers.ExceptionHandler;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import org.springframework.stereotype.Component;

@Component
public class GrpcExceptionHandler implements ServerInterceptor {

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> call, final Metadata metadata, final ServerCallHandler<ReqT, RespT> next) {
        ServerCall.Listener<ReqT> listener = next.startCall(call, metadata);
        return new ExceptionHandlingServerCallListener<>(listener, call, metadata);
    }

    private class ExceptionHandlingServerCallListener<ReqT, RespT> extends ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT> {
        private ServerCall<ReqT, RespT> serverCall;
        private Metadata metadata;

        ExceptionHandlingServerCallListener(ServerCall.Listener<ReqT> listener, ServerCall<ReqT, RespT> serverCall, Metadata metadata) {
            super(listener);
            this.serverCall = serverCall;
            this.metadata = metadata;
        }

        @Override
        public void onHalfClose() {
            try {
                super.onHalfClose();
            } catch (Exception ex) {
                handleException(ex, serverCall, metadata);
                throw ex;
            }
        }

        @Override
        public void onReady() {
            try {
                super.onReady();
            } catch (Exception ex) {
                handleException(ex, serverCall, metadata);
                throw ex;
            }
        }

        private void handleException(final Exception exception, final ServerCall<ReqT, RespT> serverCall, final Metadata metadata) {
            ExceptionHandler.logAndrenderErrorResponse(exception, serverCall.getMethodDescriptor().getFullMethodName());
            serverCall.close(io.grpc.Status.INVALID_ARGUMENT.withDescription(exception.getMessage()), metadata);
        }
    }
}
