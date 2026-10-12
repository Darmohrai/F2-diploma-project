package org.kurin.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Assigns an explicit wire id to a serialized type. Without it the id is derived from the class name,
 * so renaming or moving the class changes the id.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface KurinType {
    /**
     * @return wire id, at least 1000 and unique within the cluster
     */
    int id();
}
