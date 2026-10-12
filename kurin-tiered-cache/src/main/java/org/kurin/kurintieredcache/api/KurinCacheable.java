package org.kurin.kurintieredcache.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Caches the result of the method: a call with a key that is already cached returns the cached value without
 * running the method.
 * <p>
 * Attributes left at their defaults take the settings of the cache ({@code kurin.cache.caches.<name>.*}) and
 * then the global defaults ({@code kurin.cache.defaults.*}). Durations are written as {@code PT5M} or {@code 5m}.
 * Cache plain data objects, not managed persistence entities.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface KurinCacheable {

    String cacheName();

    /**
     * SpEL expression of the key: {@code #paramName}, {@code #a0}, {@code #p0}, {@code #root.methodName},
     * {@code #root.target}. Empty means a key derived from the class, the method and all arguments. An
     * expression that fails or yields null raises {@code KurinCacheKeyException}.
     */
    String key() default "";

    /**
     * Tiers of the cache, when the cache is not configured by properties. Empty means the configured tiers.
     */
    CacheTier[] tiers() default {};

    /**
     * Time to live of a cached value. Empty means the configured one.
     */
    String ttl() default "";

    /**
     * Time to live of a cached null. Empty means the configured one, which defaults to the regular ttl.
     */
    String nullTtl() default "";

    /**
     * Random spread of the time to live in percent: each entry lives {@code ttl ± ttl * jitterPercent / 100},
     * so that entries written together do not expire together. -1 means the configured value.
     */
    int jitterPercent() default -1;

    /**
     * Cache a null result too, so that repeated lookups of something that does not exist do not reach the source.
     */
    boolean cacheNulls() default false;

    /**
     * SpEL condition on the arguments; when it is false the cache is bypassed entirely for the call.
     */
    String condition() default "";

    /**
     * SpEL condition on {@code #result}; when it is true the result is returned but not cached.
     */
    String unless() default "";

    /**
     * For this long after a value expired it is still returned at once while one caller refreshes it in the
     * background. Empty means the configured value; zero disables it.
     */
    String staleWhileRevalidate() default "";

    StampedeMode stampede() default StampedeMode.LOCAL;

    /**
     * How long a {@link StampedeMode#CLUSTER} read waits for the cluster lock.
     */
    String lockWait() default "PT5S";

    OnLockTimeout onLockTimeout() default OnLockTimeout.LOAD;
}
