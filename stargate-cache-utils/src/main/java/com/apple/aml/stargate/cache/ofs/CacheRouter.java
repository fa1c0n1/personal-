package com.apple.aml.stargate.cache.ofs;

import io.micrometer.core.annotation.Timed;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@Timed
@RestController
@RequestMapping("/cache")
public class CacheRouter {
    @Autowired
    @Lazy
    private CacheService cacheService;

    @GetMapping(value = "/topology/current")
    public Mono<List<Map>> currentTopology() {
        return cacheService.currentTopology();
    }

    @GetMapping(value = "/topology")
    public Mono<List<Map>> topology() {
        return cacheService.currentTopology();
    }

    @PostMapping(value = "/kv/update/{cacheName}/{key}")
    public Mono<Boolean> updateKVCache(@PathVariable final String cacheName, @PathVariable final String key, @RequestBody final Object value) {
        return cacheService.updateKVCache(cacheName, key, value, null);
    }

    @PostMapping(value = "/kv/updatewithttl/{cacheName}/{key}/{ttl}")
    public Boolean updateKVCacheTTL(@PathVariable final String cacheName, @PathVariable final String key, @PathVariable final Integer ttl, @RequestBody final Object value) throws Exception {
        return cacheService.updateCacheValue(cacheName, key, value, null, Duration.ofSeconds(ttl));
    }

    @PostMapping(value = "/kv/updates/{cacheName}")
    public Mono<Map<String, Boolean>> updateKVsCache(@PathVariable final String cacheName, @RequestBody final Map<String, Object> updates) {
        return cacheService.updateKVsCache(cacheName, updates, null);
    }

    @PostMapping(value = "/kv/update/{cacheName}/{key}/{valueClassName}")
    public Mono<Boolean> updateKVCacheAs(@PathVariable final String cacheName, @PathVariable final String key, @PathVariable final String valueClassName, @RequestBody final Object value) {
        return cacheService.updateKVCache(cacheName, key, value, valueClassName);
    }

    @GetMapping(value = "/kv/get/{cacheName}/{key}")
    public Mono<Object> getKVCacheValue(@PathVariable final String cacheName, @PathVariable final String key) {
        return cacheService.getKVCacheValue(cacheName, key);
    }

    @GetMapping(value = "/kv/get/{cacheName}")
    public Mono<Map<String, Object>> getKVCacheValues(@PathVariable final String cacheName, @RequestBody final List<String> keys) {
        return cacheService.getKVCacheValues(cacheName, keys);
    }

    @PostMapping(value = "/kv/delete/{cacheName}/{key}")
    public Mono<Boolean> deleteKVCacheValue(@PathVariable final String cacheName, @PathVariable final String key) {
        return cacheService.deleteKVCacheValue(cacheName, key);
    }

    @GetMapping(value = "/kv/getkeys/{cacheName}")
    public Mono<Collection<String>> getKVKeys(@PathVariable final String cacheName) {
        return cacheService.getKVKeys(cacheName);
    }

    @PostMapping(value = "/sql/createtable/{cacheName}")
    public Mono<Boolean> createTable(@PathVariable final String cacheName, @RequestBody final String sql) {
        return cacheService.executeDDL(cacheName, sql);
    }

    @PostMapping(value = "/sql/createtable/{cacheName}/{tableName}")
    public Mono<Boolean> createSQLTable(@PathVariable final String cacheName, @PathVariable final String tableName, @RequestBody final String sql) {
        return cacheService.createTableDDL(cacheName, tableName, sql);
    }

    @PostMapping(value = "/sql/createindex/{cacheName}")
    public Mono<Boolean> createIndex(@PathVariable final String cacheName, @RequestBody final String sql) {
        return cacheService.executeDDL(cacheName, sql);
    }

    @PostMapping(value = "/sql/createindex/{cacheName}/{tableName}/{indexName}")
    public Mono<Boolean> createSQLIndex(@PathVariable final String cacheName, @PathVariable final String tableName, @PathVariable final String indexName, @RequestBody final String sql) {
        return cacheService.createIndexDDL(cacheName, tableName, indexName, sql);
    }

    @PostMapping(value = "/sql/ddl/{cacheName}")
    public Mono<Boolean> executeDDL(@PathVariable final String cacheName, @RequestBody final String sql) {
        return cacheService.executeDDL(cacheName, sql);
    }

    @PostMapping(value = "/sql/dml/{cacheName}")
    public Mono<Boolean> executeDML(@PathVariable final String cacheName, @RequestBody final String dml) {
        return cacheService.executeDML(cacheName, dml);
    }

    @PostMapping(value = "/sql/dmls/{cacheName}")
    public Mono<Map<String, Boolean>> executeDMLs(@PathVariable final String cacheName, @RequestBody final List<String> dmls) {
        return cacheService.executeDMLs(cacheName, dmls);
    }

    @PostMapping(value = "/sql/query/{cacheName}")
    public Mono<List<Map>> executeQuery(@PathVariable final String cacheName, @RequestBody final String sql) {
        return cacheService.executeQuery(cacheName, sql);
    }

    @PostMapping(value = "/sql/queries/{cacheName}")
    public Mono<Map<String, List<Map>>> executeQueries(@PathVariable final String cacheName, @RequestBody final List<String> sqls) {
        return cacheService.executeQueries(cacheName, sqls);
    }

    @PostMapping(value = "/sql/pojos/{cacheName}")
    public Mono<Map<String, Boolean>> executePOJOs(@PathVariable final String cacheName, @RequestBody final List<Object> pojos) {
        return cacheService.executePOJOs(cacheName, pojos);
    }
}
