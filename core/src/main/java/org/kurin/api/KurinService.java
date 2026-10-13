package org.kurin.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Describes a state machine service: a class with {@link KurinCommand} or {@link KurinQuery} methods.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface KurinService {
    /**
     * Stable id of the service. It keys the part of a snapshot that belongs to the service, so it must not
     * change between versions. Empty means the fully qualified class name.
     */
    String value() default "";

    /**
     * Declares that the commands of this service leave nothing in the memory of the service: they only return
     * a result or act through state machine listeners. Such a service needs no {@link KurinSnapshot} and
     * {@link KurinRestore} methods.
     * <p>
     * A service with command handlers that is not stateless and has no snapshot methods fails the start of the
     * node: once the log is compacted, whatever its commands built would be missing on every node that restarts
     * or catches up from a snapshot.
     */
    boolean stateless() default false;
}
