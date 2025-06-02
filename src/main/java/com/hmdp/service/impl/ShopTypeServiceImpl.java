package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE_KEY;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE_TTL;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result listBySort() {

        String key = CACHE_SHOP_TYPE_KEY + "list";
        // query shop type list ordered by shopTypes from Redis
        List<String> shopTypeList = stringRedisTemplate.opsForList().range(key, 0, -1);

        // If found, return the list
        if (shopTypeList != null && !shopTypeList.isEmpty()) {
            // Convert the list to Result type and return
            List<ShopType> shopTypes = shopTypeList.stream()
                    .map(json -> JSONUtil.toBean(json, ShopType.class))
                    .collect(Collectors.toList());
            return Result.ok(shopTypes);
        }

        // If not found, query from database
        List<ShopType> shopTypes = query().orderByAsc("sort").list();

        // If found in database, cache it and return the list
        if (!shopTypes.isEmpty()) {
            List<String> jsonList = shopTypes.stream()
                    .map(JSONUtil::toJsonStr)
                    .collect(Collectors.toList());
            // Cache the list in Redis
            stringRedisTemplate.opsForList().rightPushAll(key, jsonList);
            stringRedisTemplate.expire(key, CACHE_SHOP_TYPE_TTL, TimeUnit.MINUTES);

            return Result.ok(shopTypes);
        }

        // If not found in database, return an empty list
        return Result.ok(Collections.emptyList());
    }
}
