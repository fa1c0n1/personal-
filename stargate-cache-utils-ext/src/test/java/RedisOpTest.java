import com.apple.aml.stargate.cache.utils.RedisUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.redisson.api.RMapCache;
import org.redisson.api.RSet;
import org.redisson.api.RSetMultimap;
import org.redisson.api.RedissonClient;

import java.util.Set;

import static com.apple.aml.stargate.common.constants.CommonConstants.OfsConstants.CACHE_NAME_KV;

@Disabled
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RedisOpTest {

    private static RedissonClient redisClient;
    private static RMapCache<String, Object> cache;
    private static RSetMultimap<String, Object> multimap;

    @BeforeAll
    public static void setup() throws Exception {
        redisClient = RedisUtils.initRedisClient();
        cache = redisClient.getMapCache(CACHE_NAME_KV);
        multimap = redisClient.getSetMultimap("multimap");

        cleanup();
    }

    private static void cleanup() {
        cache.delete();
        multimap.delete();
    }

    @AfterAll
    public static void teardown() {
        cleanup();
        redisClient.shutdown();
    }

    @Test
    @Order(1)
    public void testPut() {
        Object value = cache.put("key", "value");
        Assertions.assertNull(value);
    }

    @Test
    @Order(2)
    public void testGet() {
        Object value = cache.get("key");
        Assertions.assertNotNull(value);
        Assertions.assertEquals("value", value);
    }

    @Test
    @Order(2)
    public void testGetKeys() {
        Set<String> keys = cache.readAllKeySet();
        Assertions.assertNotNull(keys);
        Assertions.assertTrue(keys.contains("key"));
    }

    @Test
    @Order(3)
    public void testRemove() {
        Object value = cache.remove("key");
        Assertions.assertNotNull(value);
        Assertions.assertEquals("value", value);
        value = cache.get("key");
        Assertions.assertNull(value);
    }

    @Test
    @Order(1)
    public void testMultimapPut() {
        boolean value = multimap.put("key", "value");
        Assertions.assertTrue(value);
    }

    @Test
    @Order(2)
    public void testMultimapGet() {
        RSet value = multimap.get("key");
        Assertions.assertNotNull(value);
        Assertions.assertTrue(value.contains("value"));
        Set<String> keys = multimap.readAllKeySet();
        Assertions.assertNotNull(keys);
    }

    @Test
    @Order(2)
    public void testMultimapGetKeys() {
        Set<String> keys = multimap.readAllKeySet();
        Assertions.assertNotNull(keys);
        Assertions.assertTrue(keys.contains("key"));
    }

    @Test
    @Order(3)
    public void testMultimapRemove() {
        Set value = multimap.removeAll("key");
        Assertions.assertNotNull(value);
        Assertions.assertTrue(value.contains("value"));
        value = multimap.get("key");
        Assertions.assertTrue(value.isEmpty());
    }
}
