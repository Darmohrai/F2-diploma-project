package org.kurin.test;

import org.kurin.api.KurinCommand;
import org.kurin.api.KurinQuery;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * State machine used by the core tests. Counters are atomic only so that test threads can read them;
 * they are written on the Raft thread alone.
 */
public class CounterService {

    public record Add(int delta) {
    }

    public record Reset() {
    }

    public record Fail(String message) {
    }

    public record Store(byte[] data) {
    }

    public record GetValue() {
    }

    private final AtomicInteger value = new AtomicInteger();
    private final AtomicInteger failedCommands = new AtomicInteger();
    private final AtomicInteger appliedCommands = new AtomicInteger();

    @KurinCommand
    public int add(Add command) {
        appliedCommands.incrementAndGet();
        return value.addAndGet(command.delta());
    }

    @KurinCommand
    public void reset(Reset command) {
        appliedCommands.incrementAndGet();
        value.set(0);
    }

    @KurinCommand
    public void fail(Fail command) {
        appliedCommands.incrementAndGet();
        failedCommands.incrementAndGet();
        throw new IllegalStateException(command.message());
    }

    @KurinCommand
    public int store(Store command) {
        appliedCommands.incrementAndGet();
        return command.data().length;
    }

    @KurinQuery
    public int value(GetValue query) {
        return value.get();
    }

    public int value() {
        return value.get();
    }

    public int failedCommands() {
        return failedCommands.get();
    }

    public int appliedCommands() {
        return appliedCommands.get();
    }
}
