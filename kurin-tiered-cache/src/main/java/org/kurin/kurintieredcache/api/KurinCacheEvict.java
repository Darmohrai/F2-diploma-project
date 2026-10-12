package org.kurin.kurintieredcache.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Removes entries from caches after the method completed successfully.
 * <p>
 * On a {@code @KurinCommand} handler the entries are removed on every instance when the command is applied.
 * <p>
 * On any other method the eviction runs after the method returned, or, if a transaction is active, after that
 * transaction committed (and not at all if it rolled back). With {@link #replicate()} it is then carried to
 * every instance. This is the way to keep caches correct when the application writes to its own database:
 * the write happens once, on one instance, and the cache is dropped everywhere.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface KurinCacheEvict {

    String[] cacheNames();

    /**
     * SpEL expression of the key over the arguments ({@code #cmd}, {@code #a0}, parameter names) and
     * {@code #result}. May be empty if {@link #allEntries()} is set or the first argument implements
     * {@link CacheKeyProvider}.
     */
    String key() default "";

    /**
     * Clear the caches completely instead of removing one key.
     */
    boolean allEntries() default false;

    /**
     * For methods that are not command handlers: evict on all instances, not only on this one.
     */
    boolean replicate() default true;
}
