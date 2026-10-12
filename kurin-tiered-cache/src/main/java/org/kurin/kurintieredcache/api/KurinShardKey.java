package org.kurin.kurintieredcache.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * On a field: marks the field of a DTO/entity whose value selects the shard.
 * On a method parameter: marks the argument the shard is resolved from. The argument is either
 * the key itself or an object with a {@code @KurinShardKey} field.
 */
@Target({ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface KurinShardKey {
}
