package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;
import static java.lang.Thread.sleep;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        // query shop with pass-through cache
        //Shop shop = queryWithPassThrough(id);

        // query shop with mutex cache
        //Shop shop = queryWithMutex(id);

        // query shop with logical expiration cache
        //Shop shop = queryWithLogicalExpire(id);

        Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY, id,
                Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        return Result.ok(shop);

    }

    // Apply a thread pool to rebuild the cache
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public Shop queryWithLogicalExpire(Long id){
        // query shop from Redis
        String key = CACHE_SHOP_KEY + id;
        String s = stringRedisTemplate.opsForValue().get(key);

        // If not found, return null
        if (StrUtil.isBlank(s)) {
            return null;
        }

        // If found, parse the shop data
        RedisData redisData = JSONUtil.toBean(s, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();

        // Check if the cache is expired
        if (expireTime.isAfter(LocalDateTime.now())) {
            // If not expired, return the shop data
            return shop;
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
                    this.saveShopToRedis(id, 20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    // Unlock the shop data in Redis
                    unlock(lockKey);
                }
            });

        }

        // If the shop data is being refreshed, return the shop data out of expiration time
        return shop;
    }

    public Shop queryWithMutex(Long id){
        String key = CACHE_SHOP_KEY + id;
        // 1. 从 Redis 查缓存
        String s = stringRedisTemplate.opsForValue().get(key);

        // 2. 如果查到了，直接返回
        if (StrUtil.isNotBlank(s)) {
            return JSONUtil.toBean(s, Shop.class);
        }

        // 3. 如果查到的是空值，占位符，说明数据库中本来也没有，直接返回 null
        if (s != null) {
            return null;
        }

        // 4. 缓存未命中，尝试加锁
        String lockKey = LOCK_SHOP_KEY + id;

        try {
            boolean isLock = tryLock(lockKey);
            if (!isLock) {
                // 获取锁失败，休眠后递归重试（防止缓存击穿）
                sleep(50);
                return queryWithMutex(id);
            }

            // 5. 🔁 【缓存双查】加锁成功后再次检查缓存是否已经被别的线程填充
            String cacheAgain = stringRedisTemplate.opsForValue().get(key);
            if (StrUtil.isNotBlank(cacheAgain)) {
                return JSONUtil.toBean(cacheAgain, Shop.class);
            }
            if (cacheAgain != null) {
                return null;
            }

            // 6. 缓存确实未命中，从数据库查
            Shop shop = getById(id);
            if (shop != null) {
                stringRedisTemplate.opsForValue().set(key,
                        JSONUtil.toJsonStr(shop),
                        CACHE_SHOP_TTL,
                        TimeUnit.MINUTES);
                return shop;
            }

            // 7. 数据库也查不到，写入空值防止缓存穿透
            stringRedisTemplate.opsForValue().set(key,
                    "",
                    CACHE_NULL_TTL,
                    TimeUnit.MINUTES);
            return null;

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            unlock(lockKey);
        }
    }


    public Shop queryWithPassThrough(Long id) {
        // query shop from Redis
        String key = CACHE_SHOP_KEY + id;
        String s = stringRedisTemplate.opsForValue().get(key);

        // If found, return the shop data
        if (StrUtil.isNotBlank(s)) {
            return JSONUtil.toBean(s, Shop.class);
        }

        // If not found, check if it is empty, query from database only if it is not empty
        if (s != null) {
            // If the value is empty, return an error message
            return null;
        }

        // If not found in Redis, query from database
        Shop shop = getById(id);

        // If found in database, cache the shop data in Redis
        if (shop != null) {
            // Convert shop object to JSON string
            String shopJson = JSONUtil.toJsonStr(shop);
            // Cache the shop data in Redis with a timeout
            stringRedisTemplate.opsForValue().set(key, shopJson, CACHE_SHOP_TTL, TimeUnit.MINUTES);

            return shop;
        }
        // If not found in both Redis and database, write empty in Redis, then return an error message
        stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
        return null;

    }

    @Transactional
    @Override
    public Result update(Shop shop) {
        // Update the shop data in the database
        updateById(shop);

        // Delete the shop data from Redis cache
        Long id = shop.getId();
        if (id != null) {
            // ID is not null, delete the cache
            String key = CACHE_SHOP_KEY + id;
            stringRedisTemplate.delete(key);
            return Result.ok();
        }

        // ID is null, return an error message
        return Result.fail("商户ID不能为空");

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

    // save shop data in Redis
    public void saveShopToRedis(Long id, Long expireSeconds) {
        // query shop from database
        Shop cachedShop = getById(id);

        // Insert logic expiration time
        RedisData redisData = new RedisData();
        redisData.setData(cachedShop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));

        // save shop data to Redis
        String key = CACHE_SHOP_KEY + id;
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }
}
