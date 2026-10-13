package org.kurin.routing;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method, or every method of a class, as a read that may be served by a replica.
 * <p>
 * It is a permission, not an order: database access that happens inside a read-write transaction goes to the
 * primary anyway, so a write can never land on a replica by accident.
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface KurinReadOnly {

    enum Consistency {
        /**
         * Always read from the replica. The caller may not see a write it just made.
         */
        EVENTUAL,
        /**
         * Read from the replica unless the caller wrote recently and the replica may not have that write yet;
         * then read from the primary. Whether the caller wrote is carried by its consistency token, so this
         * holds across application instances.
         */
        READ_YOUR_WRITES
    }

    Consistency consistency() default Consistency.READ_YOUR_WRITES;
}
