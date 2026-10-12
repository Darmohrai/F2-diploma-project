package org.kurin.kurinlocksspringbootstarter.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Turns a bean method into a background task that runs on the leader only: repeatedly while this node leads,
 * with {@link #interval()} between runs, and not at all on the other nodes. It needs no {@code @Scheduled}.
 * <p>
 * The method may take no parameters, a {@code LeadershipToken}, or a {@code LeadershipToken} and a
 * {@code CancellationSignal}. When the node loses leadership during a run, the signal is cancelled and the
 * thread is interrupted.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface KurinLeaderTask {

    /**
     * Unique name of the task.
     */
    String name();

    /**
     * Pause between the end of one run and the start of the next, as {@code PT10S} or {@code 10s}.
     */
    String interval();
}
