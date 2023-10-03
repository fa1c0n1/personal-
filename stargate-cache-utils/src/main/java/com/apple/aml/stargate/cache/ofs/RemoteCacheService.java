package com.apple.aml.stargate.cache.ofs;

import com.apple.aml.nativeml.spec.RemoteException;
import com.apple.aml.nativeml.spec.RemoteService;

import java.time.Duration;
import java.util.Map;

public interface RemoteCacheService extends RemoteService {
    Map<String, Boolean> saveValues(final String serviceName, final Map<String, Object> map, final Duration ttl) throws RemoteException;
}
