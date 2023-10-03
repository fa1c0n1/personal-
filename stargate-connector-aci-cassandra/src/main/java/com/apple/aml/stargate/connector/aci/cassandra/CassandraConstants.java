package com.apple.aml.stargate.connector.aci.cassandra;

public interface CassandraConstants {
    interface DiscoveryService {
        String BASE_PATH = "/api/v2/";
        String CONTEXT_PATH = "cluster-endpoints";
        String REQUEST_PARAM = "format=json";
    }
}
