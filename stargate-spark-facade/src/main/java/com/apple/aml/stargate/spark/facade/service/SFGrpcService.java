package com.apple.aml.stargate.spark.facade.service;

import com.apple.aml.stargate.common.utils.JsonUtils;
import com.apple.aml.stargate.spark.facade.rpc.proto.SparkSqlProtoServiceGrpc;
import com.apple.aml.stargate.spark.facade.rpc.proto.SqlRequestPayload;
import com.apple.aml.stargate.spark.facade.rpc.proto.SqlResponsePayload;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.apple.aml.stargate.common.utils.LogUtils.logger;

@GrpcService
public class SFGrpcService extends SparkSqlProtoServiceGrpc.SparkSqlProtoServiceImplBase {

    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    @Autowired
    private SFCoreService service;

    @Override
    public void sql(SqlRequestPayload request, StreamObserver<SqlResponsePayload> responseObserver) {
        String sql = request.getSqlQuery();

        try {
            LOGGER.info("Executing Query {}", sql);
            Dataset<Row> rowDataset =  service.executeSql(sql);

            List<Map<String,Object>> result= new ArrayList<>();
            List<String> stringDataset = rowDataset.toJSON().collectAsList();
            LOGGER.info("Number Of Rows {} for given query : {}", stringDataset.size(), sql);
            ObjectMapper OBJECT_MAPPER = new ObjectMapper();
            for(String s : stringDataset){
                Map<String,Object> map;
                map = OBJECT_MAPPER.readValue(s, new TypeReference<Map<String, Object>>(){});
                result.add(map);
            }

            SqlResponsePayload sqlResponsePayload = SqlResponsePayload.newBuilder()
                    .setResponse(JsonUtils.jsonString(result))
                    .setStatusCode(1)
                    .build();
            responseObserver.onNext(sqlResponsePayload);
        } catch (Exception e) {
            LOGGER.error("Error in executing Sql ", e);
            responseObserver.onError(new Exception(e.getMessage()));
        }
        responseObserver.onCompleted();
    }
}
