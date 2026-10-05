package org.kurin.kurintieredcache.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface KurinCacheable {

    String cacheName();

    String key() default "";

    CacheTier tier() default CacheTier.TIERED_SMART;
}
