package org.kurin.kurinlocks.state;

import org.kurin.api.KurinCommand;
import org.kurin.api.KurinCommandContext;
import org.kurin.api.KurinQuery;
import org.kurin.api.KurinRestore;
import org.kurin.api.KurinService;
import org.kurin.api.KurinSnapshot;
import org.kurin.kurinlocks.command.LockCommands.AcquireLock;
import org.kurin.kurinlocks.command.LockCommands.AcquireResult;
import org.kurin.kurinlocks.command.LockCommands.CountLocks;
import org.kurin.kurinlocks.command.LockCommands.GetLock;
import org.kurin.kurinlocks.command.LockCommands.LockRecord;
import org.kurin.kurinlocks.command.LockCommands.PurgeExpiredLocks;
import org.kurin.kurinlocks.command.LockCommands.ReleaseLock;
import org.kurin.kurinlocks.command.LockCommands.RenewLock;
import org.kurin.kurinlocks.command.LockCommands.RenewResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * Replicated table of locks. Every decision is made from the log alone: the only clock is the timestamp the
 * leader stamped on the command, so all nodes grant, refuse and expire locks identically.
 */
@KurinService("kurin.locks")
public class LockStateMachine {
    private static final Logger log = LoggerFactory.getLogger(LockStateMachine.class);

    private final Map<String, LockRecord> locks = new HashMap<>();
    // Leader clocks may differ; the state machine never lets its time run backwards
    private long nowMs;
    private final List<Consumer<String>> releaseListeners = new CopyOnWriteArrayList<>();

    /**
     * Registers a listener told, on this node, whenever a key becomes free: on release, on purge and when
     * an expired record is overwritten. It runs on the Raft thread and must not block.
     */
    public void addReleaseListener(Consumer<String> listener) {
        releaseListeners.add(listener);
    }

    private long advanceClock() {
        nowMs = Math.max(nowMs, KurinCommandContext.current().timestampMs());
        return nowMs;
    }

    @KurinCommand
    public AcquireResult acquire(AcquireLock command) {
        long now = advanceClock();
        LockRecord current = locks.get(command.key());

        if (current != null && current.ownerId().equals(command.ownerId())) {
            // The same acquisition submitted again: answer as the first time
            return new AcquireResult(true, current.fencingToken(), current.expiresAtMs());
        }
        if (current != null && now < current.expiresAtMs()) {
            return AcquireResult.denied();
        }

        long fencingToken = KurinCommandContext.current().logIndex();
        LockRecord granted = new LockRecord(command.ownerId(), fencingToken, now + command.ttlMs(), now);
        locks.put(command.key(), granted);
        log.debug("Lock '{}' granted to {} with token {}{}", command.key(), command.ownerId(), fencingToken,
                current != null ? " (previous holder " + current.ownerId() + " expired)" : "");
        if (current != null) {
            // Waiters of the expired holder lost the race to this owner, but they should re-evaluate now
            notifyReleased(command.key());
        }
        return new AcquireResult(true, fencingToken, granted.expiresAtMs());
    }

    @KurinCommand
    public RenewResult renew(RenewLock command) {
        long now = advanceClock();
        LockRecord current = locks.get(command.key());
        if (current == null || !current.ownerId().equals(command.ownerId()) || now >= current.expiresAtMs()) {
            return new RenewResult(false, 0);
        }
        LockRecord renewed = new LockRecord(current.ownerId(), current.fencingToken(), now + command.ttlMs(), current.acquiredAtMs());
        locks.put(command.key(), renewed);
        return new RenewResult(true, renewed.expiresAtMs());
    }

    @KurinCommand
    public boolean release(ReleaseLock command) {
        advanceClock();
        LockRecord current = locks.get(command.key());
        if (current == null || !current.ownerId().equals(command.ownerId())) {
            return false;
        }
        locks.remove(command.key());
        log.debug("Lock '{}' released by {}", command.key(), command.ownerId());
        notifyReleased(command.key());
        return true;
    }

    @KurinCommand
    public int purgeExpired(PurgeExpiredLocks command) {
        long now = advanceClock();
        int removed = 0;
        for (Iterator<Map.Entry<String, LockRecord>> iterator = locks.entrySet().iterator(); iterator.hasNext(); ) {
            Map.Entry<String, LockRecord> entry = iterator.next();
            if (now >= entry.getValue().expiresAtMs()) {
                iterator.remove();
                removed++;
                notifyReleased(entry.getKey());
            }
        }
        if (removed > 0) {
            log.debug("Purged {} expired lock(s)", removed);
        }
        return removed;
    }

    @KurinQuery
    public LockRecord get(GetLock query) {
        return locks.get(query.key());
    }

    @KurinQuery
    public int count(CountLocks query) {
        return locks.size();
    }

    private void notifyReleased(String key) {
        for (Consumer<String> listener : releaseListeners) {
            listener.accept(key);
        }
    }

    @KurinSnapshot
    public byte[] snapshot() {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeLong(nowMs);
            out.writeInt(locks.size());
            for (Map.Entry<String, LockRecord> entry : locks.entrySet()) {
                LockRecord record = entry.getValue();
                out.writeUTF(entry.getKey());
                out.writeUTF(record.ownerId());
                out.writeLong(record.fencingToken());
                out.writeLong(record.expiresAtMs());
                out.writeLong(record.acquiredAtMs());
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
            locks.clear();
            nowMs = in.readLong();
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                String key = in.readUTF();
                locks.put(key, new LockRecord(in.readUTF(), in.readLong(), in.readLong(), in.readLong()));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
