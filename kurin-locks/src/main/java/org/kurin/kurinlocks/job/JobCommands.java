package org.kurin.kurinlocks.job;

/**
 * Commands, queries and results of the replicated job state machine.
 */
public final class JobCommands {
    private JobCommands() {
    }

    public enum JobMode {
        /**
         * A slot is claimed once and never again, even if its owner dies before finishing.
         */
        AT_MOST_ONCE,
        /**
         * If the owner does not report completion before its lease ends, another node may take the slot over
         * and run the job again.
         */
        AT_LEAST_ONCE
    }

    public enum JobStatus {
        CLAIMED,
        SUCCEEDED,
        FAILED
    }

    /**
     * @param slot        number of the time slot the run belongs to; later slots have larger numbers
     * @param leaseMs     time the owner has to report completion before the slot may be taken over
     * @param historySize number of slots to remember per job; carried by the command so that every node
     *                    prunes identically whatever its local configuration
     */
    public record ClaimJob(String jobName, long slot, String ownerId, long leaseMs, JobMode mode, int historySize) {
    }

    /**
     * @param fencingToken log index of the granting command; a takeover of the same slot gets a larger one
     */
    public record ClaimResult(boolean granted, long fencingToken) {
        public static ClaimResult denied() {
            return new ClaimResult(false, 0);
        }
    }

    public record CompleteJob(String jobName, long slot, String ownerId, boolean success, String error) {
    }

    /**
     * Query: the remembered runs of a job, oldest slot first.
     */
    public record JobHistory(String jobName) {
    }

    public record JobRun(String jobName, long slot, String ownerId, long fencingToken, long claimedAtMs,
                         long leaseUntilMs, JobStatus status, String error, long completedAtMs) {
    }
}
