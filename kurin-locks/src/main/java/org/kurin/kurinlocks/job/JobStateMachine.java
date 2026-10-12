package org.kurin.kurinlocks.job;

import org.kurin.api.KurinCommand;
import org.kurin.api.KurinCommandContext;
import org.kurin.api.KurinQuery;
import org.kurin.api.KurinRestore;
import org.kurin.api.KurinService;
import org.kurin.api.KurinSnapshot;
import org.kurin.kurinlocks.job.JobCommands.ClaimJob;
import org.kurin.kurinlocks.job.JobCommands.ClaimResult;
import org.kurin.kurinlocks.job.JobCommands.CompleteJob;
import org.kurin.kurinlocks.job.JobCommands.JobHistory;
import org.kurin.kurinlocks.job.JobCommands.JobMode;
import org.kurin.kurinlocks.job.JobCommands.JobRun;
import org.kurin.kurinlocks.job.JobCommands.JobStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Replicated record of which node ran which slot of which job. A slot is the unit of "run once": the cluster
 * grants each (job, slot) to one claimer, decided from the log alone.
 */
@KurinService("kurin.jobs")
public class JobStateMachine {
    private static final Logger log = LoggerFactory.getLogger(JobStateMachine.class);

    private static final class Job {
        final TreeMap<Long, JobRun> runs = new TreeMap<>();
        // Slots at or below this number were pruned from the history; they can never be claimed again
        long prunedThroughSlot = Long.MIN_VALUE;
    }

    private final Map<String, Job> jobs = new HashMap<>();
    private long nowMs;

    private long advanceClock() {
        nowMs = Math.max(nowMs, KurinCommandContext.current().timestampMs());
        return nowMs;
    }

    @KurinCommand
    public ClaimResult claim(ClaimJob command) {
        long now = advanceClock();
        Job job = jobs.computeIfAbsent(command.jobName(), name -> new Job());
        if (command.slot() <= job.prunedThroughSlot) {
            return ClaimResult.denied();
        }

        JobRun existing = job.runs.get(command.slot());
        if (existing != null) {
            if (existing.ownerId().equals(command.ownerId())) {
                // The same claim submitted again
                return new ClaimResult(existing.status() == JobStatus.CLAIMED, existing.fencingToken());
            }
            boolean abandoned = existing.status() == JobStatus.CLAIMED && now >= existing.leaseUntilMs();
            if (command.mode() != JobMode.AT_LEAST_ONCE || !abandoned) {
                return ClaimResult.denied();
            }
            log.debug("Slot {} of job '{}' is taken over from {} by {}", command.slot(), command.jobName(),
                    existing.ownerId(), command.ownerId());
        }

        long fencingToken = KurinCommandContext.current().logIndex();
        job.runs.put(command.slot(), new JobRun(command.jobName(), command.slot(), command.ownerId(), fencingToken,
                now, now + command.leaseMs(), JobStatus.CLAIMED, null, 0));

        int historySize = Math.max(1, command.historySize());
        while (job.runs.size() > historySize) {
            job.prunedThroughSlot = Math.max(job.prunedThroughSlot, job.runs.pollFirstEntry().getKey());
        }
        // The slot just claimed may itself be older than everything that was kept
        if (command.slot() <= job.prunedThroughSlot) {
            return ClaimResult.denied();
        }
        return new ClaimResult(true, fencingToken);
    }

    @KurinCommand
    public boolean complete(CompleteJob command) {
        long now = advanceClock();
        Job job = jobs.get(command.jobName());
        JobRun run = job != null ? job.runs.get(command.slot()) : null;
        if (run == null || !run.ownerId().equals(command.ownerId()) || run.status() != JobStatus.CLAIMED) {
            // Not the current owner any more (the slot was taken over), or already completed
            return false;
        }
        job.runs.put(command.slot(), new JobRun(run.jobName(), run.slot(), run.ownerId(), run.fencingToken(),
                run.claimedAtMs(), run.leaseUntilMs(), command.success() ? JobStatus.SUCCEEDED : JobStatus.FAILED,
                command.error(), now));
        return true;
    }

    @KurinQuery
    public List<JobRun> history(JobHistory query) {
        Job job = jobs.get(query.jobName());
        return job != null ? new ArrayList<>(job.runs.values()) : new ArrayList<>();
    }

    @KurinSnapshot
    public byte[] snapshot() {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeLong(nowMs);
            out.writeInt(jobs.size());
            for (Map.Entry<String, Job> entry : jobs.entrySet()) {
                Job job = entry.getValue();
                out.writeUTF(entry.getKey());
                out.writeLong(job.prunedThroughSlot);
                out.writeInt(job.runs.size());
                for (JobRun run : job.runs.values()) {
                    out.writeLong(run.slot());
                    out.writeUTF(run.ownerId());
                    out.writeLong(run.fencingToken());
                    out.writeLong(run.claimedAtMs());
                    out.writeLong(run.leaseUntilMs());
                    out.writeUTF(run.status().name());
                    out.writeBoolean(run.error() != null);
                    if (run.error() != null) {
                        out.writeUTF(run.error());
                    }
                    out.writeLong(run.completedAtMs());
                }
            }
            out.flush();
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @KurinRestore
    public void restore(byte[] data) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
            jobs.clear();
            nowMs = in.readLong();
            int jobCount = in.readInt();
            for (int i = 0; i < jobCount; i++) {
                String jobName = in.readUTF();
                Job job = new Job();
                job.prunedThroughSlot = in.readLong();
                int runCount = in.readInt();
                for (int j = 0; j < runCount; j++) {
                    long slot = in.readLong();
                    String ownerId = in.readUTF();
                    long fencingToken = in.readLong();
                    long claimedAtMs = in.readLong();
                    long leaseUntilMs = in.readLong();
                    JobStatus status = JobStatus.valueOf(in.readUTF());
                    String error = in.readBoolean() ? in.readUTF() : null;
                    long completedAtMs = in.readLong();
                    job.runs.put(slot, new JobRun(jobName, slot, ownerId, fencingToken, claimedAtMs, leaseUntilMs,
                            status, error, completedAtMs));
                }
                jobs.put(jobName, job);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
