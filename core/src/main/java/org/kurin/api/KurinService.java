package org.kurin.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Gives a state machine service a stable id. The id keys the part of a snapshot that belongs to the service,
 * so it must not change between versions. Without it the fully qualified class name is used.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface KurinService {
    String value();
}
