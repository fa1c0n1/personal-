package com.apple.aml.stargate.app.service;

import com.apple.aml.dataplatform.client.rpc.stargate.proto.ConsumeDhariMessagePayload;
import com.apple.aml.dataplatform.client.rpc.stargate.proto.ConsumeDhariMessagesResponse;
import com.apple.aml.dataplatform.client.rpc.stargate.proto.StargateAdminProtoServiceGrpc.StargateAdminProtoServiceImplBase;
import com.apple.aml.dataplatform.client.rpc.stargate.proto.StargatePing;
import com.apple.aml.dataplatform.client.rpc.stargate.proto.StargateStatus;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.common.utils.LogUtils.logger;

@Service
public class GrpcService extends StargateAdminProtoServiceImplBase {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    @Autowired
    private DhariProxyService dhariProxyService;

    @Override
    public void ping(StargatePing request, StreamObserver<StargateStatus> observer) {
        LOGGER.debug("grpc received ping");
        observer.onNext(StargateStatus.newBuilder().setStatus(true).build());
        observer.onCompleted();
    }

    @Override
    public void consumeDhariMessages(ConsumeDhariMessagePayload request, StreamObserver<ConsumeDhariMessagesResponse> observer) {
        List<String> output = dhariProxyService.consumeMessages(request.getAppId(), request.getPublisherAppId(), request.getConsumerId(), request.getNoOfMessages(), request.getTopic(), request.getPartition(), request.getOffset(), request.getSchemaId(), request.getPublishFormat(), null)
                .map(messages -> messages.stream().map(m -> m.toString()).collect(Collectors.toList())).block();
        observer.onNext(ConsumeDhariMessagesResponse.newBuilder().addAllJson(output).build());
        observer.onCompleted();
    }
}
