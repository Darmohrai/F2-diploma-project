package org.kurin.kurinlocksspringbootstarter.annotation;

import org.kurin.kurinlocks.job.JobCommands.JobMode;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Makes a method that every instance triggers on the same schedule run on one instance per time slot.
 * Put it next to {@code @Scheduled}:
 *
 * <pre>{@code
 * @Scheduled(cron = "0 *&#47;5 * * * *")
 * @KurinScheduledOnce(name = "sync-rates", slot = "PT5M")
 * public void syncRates() { ... }
 * }</pre>
 * <p>
 * Each instance rounds its clock to the nearest multiple of {@link #slot()} and asks the cluster for that slot;
 * one of them is granted and runs the method, the others skip. Set the slot to the period of the schedule.
 * The clocks of the instances must agree to well within half a slot, otherwise two instances can compute
 * different slots for the same trigger and both run. Inside the method {@code KurinJobContext.current()}
 * gives the slot and its fencing token.
 * <p>
 * The method must return void or a reference type; on a skipped run it returns null (or an empty Optional).
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface KurinScheduledOnce {

    /**
     * Name of the job, shared by all instances.
     */
    String name();

    /**
     * Length of a time slot, as {@code PT5M}, {@code 5m} or a number of milliseconds. May be a property
     * placeholder, {@code "${rates.sync.slot:5m}"}, resolved when the context starts.
     */
    String slot() default "PT5M";

    /**
     * AT_MOST_ONCE never repeats a slot, even if the instance that won it crashed. AT_LEAST_ONCE lets another
     * instance run the slot again when the winner did not finish within {@link #lease()}.
     */
    JobMode mode() default JobMode.AT_MOST_ONCE;

    /**
     * Time the winner has to finish before the slot counts as abandoned. Only used with AT_LEAST_ONCE.
     * Same formats as {@link #slot()}, placeholders included.
     */
    String lease() default "PT1M";
}
