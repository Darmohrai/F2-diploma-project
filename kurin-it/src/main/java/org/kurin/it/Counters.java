package org.kurin.it;

import org.kurin.api.KurinCommand;
import org.kurin.api.KurinQuery;
import org.kurin.api.KurinRestore;
import org.kurin.api.KurinService;
import org.kurin.api.KurinSnapshot;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;

/**
 * State replicated through Raft: the same number on every instance.
 */
@Component
@KurinService("it-counters")
public class Counters {
    public record Add(int delta) {
    }

    public record Read() {
    }

    private int value;

    @KurinCommand
    public int add(Add command) {
        value += command.delta();
        return value;
    }

    @KurinQuery
    public int read(Read query) {
        return value;
    }

    @KurinSnapshot
    public byte[] snapshot() {
        return ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
    }

    @KurinRestore
    public void restore(byte[] snapshot) {
        value = ByteBuffer.wrap(snapshot).getInt();
    }
}
