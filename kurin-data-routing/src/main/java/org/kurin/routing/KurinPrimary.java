package org.kurin.routing;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Forces a method, or every method of a class, to use the primary: for reads that must see the latest
 * committed data whoever wrote it. Overrides a surrounding {@link KurinReadOnly}.
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface KurinPrimary {
}
