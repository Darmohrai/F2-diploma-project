package org.kurin.kurinlocks.api;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Gives code running under a declarative lock access to its {@link LockHandle}, for example to read the
 * fencing token.
 */
public final class KurinLockContext {
    private static final ThreadLocal<Deque<LockHandle>> HANDLES = ThreadLocal.withInitial(ArrayDeque::new);

    private KurinLockContext() {
    }

    /**
     * @return the handle of the innermost lock taken for the current call
     * @throws IllegalStateException if the current thread runs under no declarative lock
     */
    public static LockHandle current() {
        LockHandle handle = HANDLES.get().peek();
        if (handle == null) {
            throw new IllegalStateException("No Kurin lock is held by the current call");
        }
        return handle;
    }

    public static boolean isActive() {
        return !HANDLES.get().isEmpty();
    }

    public static void push(LockHandle handle) {
        HANDLES.get().push(handle);
    }

    public static void pop() {
        Deque<LockHandle> handles = HANDLES.get();
        handles.pop();
        if (handles.isEmpty()) {
            HANDLES.remove();
        }
    }
}
