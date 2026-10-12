package org.kurin.kurintieredcache.tier;

import java.util.function.Consumer;

/**
 * Stops calling a failing dependency. After {@code failureThreshold} consecutive failures the circuit opens and
 * every call is refused for {@code openNanos}. Then exactly one probe call is let through: its success closes
 * the circuit, its failure opens it again.
 */
public class CircuitBreaker {

    public enum State {
        CLOSED,
        OPEN,
        HALF_OPEN
    }

    private final int failureThreshold;
    private final long openNanos;
    private final Consumer<State> onStateChange;

    private State state = State.CLOSED;
    private int consecutiveFailures;
    private long openedAtNanos;
    private boolean probeInFlight;

    public CircuitBreaker(int failureThreshold, long openNanos, Consumer<State> onStateChange) {
        if (failureThreshold <= 0 || openNanos <= 0) {
            throw new IllegalArgumentException("failureThreshold and the open duration must be positive");
        }
        this.failureThreshold = failureThreshold;
        this.openNanos = openNanos;
        this.onStateChange = onStateChange;
    }

    /**
     * @return true if the caller may make a call now. Each permitted call must be reported with
     * {@link #onSuccess()} or {@link #onFailure()}.
     */
    public synchronized boolean tryAcquire() {
        if (state == State.CLOSED) {
            return true;
        }
        if (state == State.OPEN && System.nanoTime() - openedAtNanos >= openNanos) {
            transition(State.HALF_OPEN);
            probeInFlight = false;
        }
        if (state == State.HALF_OPEN && !probeInFlight) {
            probeInFlight = true;
            return true;
        }
        return false;
    }

    public synchronized void onSuccess() {
        consecutiveFailures = 0;
        probeInFlight = false;
        if (state != State.CLOSED) {
            transition(State.CLOSED);
        }
    }

    public synchronized void onFailure() {
        probeInFlight = false;
        if (state == State.HALF_OPEN || (state == State.CLOSED && ++consecutiveFailures >= failureThreshold)) {
            consecutiveFailures = 0;
            openedAtNanos = System.nanoTime();
            transition(State.OPEN);
        }
    }

    public synchronized State state() {
        // Reported as it would be seen by the next caller
        if (state == State.OPEN && System.nanoTime() - openedAtNanos >= openNanos) {
            return State.HALF_OPEN;
        }
        return state;
    }

    private void transition(State next) {
        if (state != next) {
            state = next;
            onStateChange.accept(next);
        }
    }
}
