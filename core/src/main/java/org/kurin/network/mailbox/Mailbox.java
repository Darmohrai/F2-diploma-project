package org.kurin.network.mailbox;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

public class Mailbox {

    private final ConcurrentLinkedQueue<Runnable> queue = new ConcurrentLinkedQueue<>();

    private final AtomicBoolean isProcessing = new AtomicBoolean(false);

    private final Executor executor;

    public Mailbox(Executor executor) {
        this.executor = executor;
    }

    public void submit(Runnable task) {
        queue.offer(task);
        schedule();
    }

    private void schedule() {
        if (isProcessing.compareAndSet(false, true)) {
            executor.execute(this::drain);
        }
    }

    private void drain() {
        try {
            Runnable task;
            while ((task = queue.poll()) != null) {
                task.run();
            }
        } finally {
            isProcessing.set(false);

            if (!queue.isEmpty()) {
                schedule();
            }
        }
    }
}
