package com.hmdp.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class MvcConfig implements WebMvcConfigurer {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(new com.hmdp.utils.LoginInterceptor())
                .excludePathPatterns("/shop/**",
                                    "/voucher/**",
                                    "/upload/**",
                                    "/blog/hot",
                                    "/user/code",
                                    "/user/login",
                                    "/shop-type/**").order(1);
        registry.addInterceptor(new com.hmdp.utils.TokenRefresherInterceptor(stringRedisTemplate)).order(0);
    }
}
