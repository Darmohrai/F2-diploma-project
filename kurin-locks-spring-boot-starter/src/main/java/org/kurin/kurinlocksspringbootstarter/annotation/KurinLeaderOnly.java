package org.kurin.kurinlocksspringbootstarter.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Runs the method only on the node that is provably the leader when the call starts: it holds a valid leader
 * lease and has established its term. Inside the method {@code KurinLeadership.currentToken()} gives the
 * token, whose term is a fencing token for external systems.
 * <p>
 * The check is made once, at the start of the call. A method that runs for longer than the lease may still be
 * running after another node became leader.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface KurinLeaderOnly {

    enum OnNotLeader {
        /**
         * Do not run the method; return null (or an empty Optional). Not allowed for primitive return types.
         */
        SKIP,
        /**
         * Throw {@code KurinNotLeaderException}, which carries the id of the known leader.
         */
        THROW
    }

    OnNotLeader onNotLeader() default OnNotLeader.SKIP;
}
