package org.kurin.kurintieredcache.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Stores the result of the method in a cache.
 * <p>
 * On a {@code @KurinCommand} handler the value is stored on every instance when the command is applied.
 * On any other method it is stored on the instance that ran the method.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface KurinCachePut {

    String cacheName();

    /**
     * SpEL expression of the key over the arguments ({@code #cmd}, {@code #a0}, parameter names) and
     * {@code #result}. May be empty only if the first argument implements {@link CacheKeyProvider}.
     */
    String key() default "";

    /**
     * Time to live of the stored value. Empty means the configured one.
     */
    String ttl() default "";
}
