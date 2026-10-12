package org.kurin.kurinlocks.leader;

import org.kurin.api.LeadershipToken;
import org.kurin.raft.KurinNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Background tasks that live on the leader only. While this node holds established leadership each registered
 * task runs repeatedly with its interval between runs. When leadership is lost, running tasks are cancelled
 * (signal and thread interrupt) and no new runs start until the node becomes leader again.
 * <p>
 * A run starts only under a valid leader lease, but nothing can stop a run instantly: a task that modifies
 * external systems should carry {@code token.term()} as a fencing token.
 */
public class KurinLeaderTasks implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(KurinLeaderTasks.class);

    private final class Registration {
        final String name;
        final long intervalMs;
        final LeaderTask task;
        volatile Future<?> schedule;
        volatile Signal runningSignal;
        volatile Thread runningThread;

        Registration(String name, Duration interval, LeaderTask task) {
            this.name = name;
            this.intervalMs = Math.max(1, interval.toMillis());
            this.task = task;
        }

        void runOnce() {
            Optional<LeadershipToken> token = node.leadership();
            if (token.isEmpty()) {
                return;
            }
            Signal signal = new Signal();
            runningSignal = signal;
            runningThread = Thread.currentThread();
            try {
                KurinLeadership.callWith(token.get(), () -> {
                    task.run(token.get(), signal);
                    return null;
                });
            } catch (InterruptedException e) {
                log.debug("Leader task '{}' was interrupted", name);
            } catch (Exception e) {
                if (signal.isCancelled()) {
                    log.debug("Leader task '{}' stopped after leadership was lost: {}", name, e.toString());
                } else {
                    log.warn("Leader task '{}' failed", name, e);
                }
            } finally {
                runningThread = null;
                runningSignal = null;
                // The interrupt of a cancellation must not leak into the next task on this pool thread
                Thread.interrupted();
            }
        }

        void cancel() {
            Future<?> current = schedule;
            if (current != null) {
                current.cancel(false);
                schedule = null;
            }
            Signal signal = runningSignal;
            Thread thread = runningThread;
            if (signal != null) {
                signal.cancelled = true;
                if (thread != null) {
                    thread.interrupt();
                }
            }
        }
    }

    private static final class Signal implements CancellationSignal {
        volatile boolean cancelled;

        @Override
        public boolean isCancelled() {
            return cancelled;
        }
    }

    private final KurinNode node;
    private final ScheduledExecutorService scheduler;
    private final Map<String, Registration> registrations = new ConcurrentHashMap<>();
    private final Duration checkInterval;
    private volatile boolean leading;
    private volatile boolean started;
    private Future<?> monitor;

    public KurinLeaderTasks(KurinNode node) {
        // Leadership is re-evaluated several times per heartbeat-sized period, so losing it is noticed quickly
        this(node, Duration.ofMillis(Math.max(10, node.config().getHeartbeatIntervalMs() / 2)));
    }

    public KurinLeaderTasks(KurinNode node, Duration checkInterval) {
        this.node = node;
        this.checkInterval = checkInterval;
        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(4, task -> {
            Thread thread = new Thread(task, "kurin-leader-task-" + threadNumber.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
        executor.setRemoveOnCancelPolicy(true);
        this.scheduler = executor;
    }

    /**
     * Registers a task. It first runs as soon as this node is (or becomes) the leader, then again
     * {@code interval} after each run ends.
     *
     * @throws IllegalArgumentException if a task with this name is already registered
     */
    public synchronized void register(String name, Duration interval, LeaderTask task) {
        Registration registration = new Registration(name, interval, task);
        if (registrations.putIfAbsent(name, registration) != null) {
            throw new IllegalArgumentException("A leader task named '" + name + "' is already registered");
        }
        if (leading) {
            schedule(registration);
        }
    }

    public synchronized void unregister(String name) {
        Registration registration = registrations.remove(name);
        if (registration != null) {
            registration.cancel();
        }
    }

    /**
     * Starts watching the leadership of the node.
     */
    public synchronized void start() {
        if (started) {
            return;
        }
        started = true;
        monitor = scheduler.scheduleWithFixedDelay(this::checkLeadership, 0, checkInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    private synchronized void checkLeadership() {
        try {
            boolean leaderNow = node.leadership().isPresent();
            if (leaderNow && !leading) {
                leading = true;
                log.debug("Node {} leads: starting {} leader task(s)", node.nodeId(), registrations.size());
                registrations.values().forEach(this::schedule);
            } else if (!leaderNow && leading) {
                leading = false;
                log.debug("Node {} no longer leads: cancelling leader tasks", node.nodeId());
                registrations.values().forEach(Registration::cancel);
            }
        } catch (RuntimeException e) {
            log.warn("Leadership check failed", e);
        }
    }

    private void schedule(Registration registration) {
        registration.schedule = scheduler.scheduleWithFixedDelay(registration::runOnce, 0, registration.intervalMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized void close() {
        if (monitor != null) {
            monitor.cancel(false);
        }
        leading = false;
        registrations.values().forEach(Registration::cancel);
        scheduler.shutdownNow();
    }
}
