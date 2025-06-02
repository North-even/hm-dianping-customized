package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Slf4j
@Component
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;

    // Apply a thread pool to rebuild the cache
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);


    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long expire, TimeUnit timeUnit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), expire, timeUnit);
    }

    public void setWithLogicalExpire(String key, Object value, Long expire, TimeUnit timeUnit) {
        // combine the value with logical expiration
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(timeUnit.toSeconds(expire)));

        // serialize the combined data to JSON and store it in Redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    public <R, ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback,
                                          Long expire, TimeUnit timeUnit) {
        String key = keyPrefix + id;
        // query shop from Redis
        String json = stringRedisTemplate.opsForValue().get(key);

        // If found, return the shop data
        if (StrUtil.isNotBlank(json)) {
            return JSONUtil.toBean(json, type);
        }

        // If not found, check if it is empty, query from database only if it is not empty
        if (json != null) {
            // If the value is empty, return an error message
            return null;
        }

        // If not found in Redis, query from database
        R r = dbFallback.apply(id);

        // If found in database, cache the shop data in Redis
        if (r != null) {
            // Serialize the shop data to JSON and store it in Redis with an expiration time
            this.stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(r), expire, timeUnit);
            return r;
        }
        // If not found in both Redis and database, write empty in Redis, then return an error message
        stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
        return null;

    }

    public <ID, R> R queryWithLogicalExpire(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback,
                                        Long expire, TimeUnit timeUnit){
        // query shop from Redis
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);

        // If not found, return null
        if (StrUtil.isBlank(json)) {
            return null;
        }

        // If found, parse the shop data
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();

        // Check if the cache is expired
        if (expireTime.isAfter(LocalDateTime.now())) {
            // If not expired, return the shop data
            return r;
        }
        // If expired, try to refresh it
        String lockKey = LOCK_SHOP_KEY + id;

        // Rebuild the cache with mutex
        boolean triedLock = tryLock(lockKey);

        // Check if the shop data is already being refreshed by another thread
        if (triedLock) {
            // If not being refreshed, apply a thread to refresh the cache
            CACHE_REBUILD_EXECUTOR.execute(() -> {
                try {
                    R r1 = dbFallback.apply(id);
                    this.setWithLogicalExpire(key, r1, expire, timeUnit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    // Unlock the shop data in Redis
                    unlock(lockKey);
                }
            });

        }

        // If the shop data is being refreshed, return the shop data out of expiration time
        return r;
    }

    // lock shop data in Redis
    private boolean tryLock(String key) {
        Boolean success = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.MINUTES);
        return BooleanUtil.isTrue(success);
    }

    // unlock shop data in Redis
    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }

}
