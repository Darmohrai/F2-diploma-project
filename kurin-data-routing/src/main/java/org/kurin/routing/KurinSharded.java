package org.kurin.routing;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Routes the database access of a method to the shard that owns the given key. An alternative to marking
 * a parameter with {@link KurinShardKey}, for keys that have to be computed.
 *
 * <pre>{@code
 * @KurinSharded(key = "#order.customerId")
 * public void save(Order order) { ... }
 * }</pre>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface KurinSharded {

    /**
     * SpEL expression of the shard key over the arguments ({@code #name}, {@code #a0}, {@code #p0}).
     * A null result is an error.
     */
    String key();
}
