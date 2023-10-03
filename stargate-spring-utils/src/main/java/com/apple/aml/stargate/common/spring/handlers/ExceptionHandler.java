package com.apple.aml.stargate.common.spring.handlers;

import com.apple.aml.stargate.common.exceptions.AcceptedStateException;
import com.apple.aml.stargate.common.exceptions.GenericException;
import com.apple.aml.stargate.common.exceptions.GenericRuntimeException;
import com.apple.aml.stargate.common.exceptions.ResourceNotFoundException;
import com.apple.aml.stargate.common.exceptions.UnauthorizedException;
import org.slf4j.Logger;
import org.springframework.boot.autoconfigure.web.WebProperties;
import org.springframework.boot.autoconfigure.web.reactive.error.AbstractErrorWebExceptionHandler;
import org.springframework.boot.web.error.ErrorAttributeOptions;
import org.springframework.boot.web.reactive.error.ErrorAttributes;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Primary;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerCodecConfigurer;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.server.RequestPredicates;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;

@Component
@Order(-2)
@Primary
public class ExceptionHandler extends AbstractErrorWebExceptionHandler {

    private static final Set<Class<? extends Exception>> DEFAULT_WRAPPER_EXCEPTIONS = Set.of(CompletionException.class, ExecutionException.class);
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private final ErrorAttributeOptions options = ErrorAttributeOptions.defaults().including(ErrorAttributeOptions.Include.STACK_TRACE);

    public ExceptionHandler(final ErrorAttributes errorAttributes, final WebProperties webProperties, final ApplicationContext applicationContext, final ServerCodecConfigurer configurer) {
        super(errorAttributes, webProperties.getResources(), applicationContext);
        this.setMessageWriters(configurer.getWriters());
        this.setMessageReaders(configurer.getReaders());
    }

    @Override
    protected RouterFunction<ServerResponse> getRoutingFunction(final ErrorAttributes errorAttributes) {
        return RouterFunctions.route(RequestPredicates.all(), this::renderErrorResponse);
    }

    @SuppressWarnings("unchecked")
    private Mono<ServerResponse> renderErrorResponse(final ServerRequest request) {
        return logAndrenderErrorResponse(getError(request), request.path());
    }

    @SuppressWarnings("unchecked")
    public static Mono<ServerResponse> logAndrenderErrorResponse(final Throwable thrownException, final String path) {
        HttpStatus httpStatus = HttpStatus.INTERNAL_SERVER_ERROR;
        String statusMessage;
        final String requestPath = String.valueOf(path);
        try {
            final Throwable actualException;
            if (thrownException.getCause() != null) {
                actualException = DEFAULT_WRAPPER_EXCEPTIONS.stream().filter(clazz -> clazz.isAssignableFrom(thrownException.getClass())).findFirst().map(clazz -> thrownException.getCause()).orElse(thrownException);
            } else {
                actualException = thrownException;
            }
            if (actualException instanceof AcceptedStateException) {
                AcceptedStateException ae = (AcceptedStateException) actualException;
                LOGGER.debug("Intermediate/Accepted State", actualException, Map.of("uri", requestPath, ERROR_MESSAGE, ae.getMessage()), ae.getDebugPojo() != null ? ae.getDebugPojo() : ae.getDebugInfo());
                httpStatus = HttpStatus.ACCEPTED;
                statusMessage = ae.getMessage();
            } else if (actualException instanceof ResourceNotFoundException) {
                ResourceNotFoundException re = (ResourceNotFoundException) actualException;
                LOGGER.error("Error processing resource not found", actualException, Map.of("uri", requestPath, ERROR_MESSAGE, re.getMessage()), re.getDebugPojo() != null ? re.getDebugPojo() : re.getDebugInfo());
                httpStatus = HttpStatus.NOT_FOUND;
                statusMessage = re.getMessage();
            } else if (actualException instanceof UnauthorizedException) {
                UnauthorizedException ue = (UnauthorizedException) actualException;
                LOGGER.error("Error processing unauthorized request", actualException, Map.of("uri", requestPath, ERROR_MESSAGE, ue.getMessage()), ue.getDebugPojo() != null ? ue.getDebugPojo() : ue.getDebugInfo());
                httpStatus = HttpStatus.UNAUTHORIZED;
                statusMessage = ue.getMessage();
            } else if (actualException instanceof GenericException) {
                GenericException be = (GenericException) actualException;
                LOGGER.error("Error processing bad request", actualException, Map.of("uri", requestPath, ERROR_MESSAGE, be.getMessage()), be.getDebugPojo() != null ? be.getDebugPojo() : be.getDebugInfo());
                httpStatus = HttpStatus.BAD_REQUEST;
                statusMessage = be.getMessage();
            } else if (actualException instanceof GenericRuntimeException) {
                GenericRuntimeException re = (GenericRuntimeException) actualException;
                GenericException be = re.getException();
                LOGGER.error("Error processing bad request", actualException, Map.of("uri", requestPath, ERROR_MESSAGE, be.getMessage()), be.getDebugPojo() != null ? be.getDebugPojo() : be.getDebugInfo());
                httpStatus = HttpStatus.BAD_REQUEST;
                statusMessage = be.getMessage();
            } else if (actualException instanceof ResponseStatusException) {
                ResponseStatusException rse = (ResponseStatusException) actualException;
                LOGGER.error("Error processing request", actualException, Map.of("uri", requestPath, ERROR_MESSAGE, rse.getMessage(), "reason", String.valueOf(rse.getReason()), "httpStatus", String.valueOf(rse.getStatus())));
                httpStatus = rse.getStatus();
                statusMessage = rse.getMessage();
            } else {
                LOGGER.error("Error processing request", actualException, Map.of("uri", requestPath, ERROR_MESSAGE, String.valueOf(actualException.getMessage())));
                LOGGER.error("Thrown exception", thrownException);
                statusMessage = "Error";
            }
        } catch (Exception e) {
            LOGGER.error("Could not translate exception to valid HttpReponse statusMessage ", Map.of("uri", requestPath, ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
            statusMessage = "UnKnown";
        }
        return ServerResponse.status(httpStatus).contentType(MediaType.APPLICATION_JSON).body(BodyInserters.fromValue(Map.of("statusMessage", statusMessage)));
    }

}
