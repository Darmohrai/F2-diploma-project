package org.kurin.network.serializer;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import com.esotericsoftware.kryo.kryo5.objenesis.strategy.StdInstantiatorStrategy;
import com.esotericsoftware.kryo.kryo5.util.DefaultInstantiatorStrategy;
import org.kurin.network.model.NodeAddress;
import org.kurin.raft.log.LogEntry;
import org.kurin.raft.rpc.*;

import java.io.*;
import java.util.List;

public class KryoSerializer {

    private final ThreadLocal<Kryo> KRYO_THREAD_LOCAL;

    public KryoSerializer(List<Class<?>> customClasses) {
        this.KRYO_THREAD_LOCAL = ThreadLocal.withInitial(() -> {
            Kryo kryo = new Kryo();

            kryo.setRegistrationRequired(true);
            kryo.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));

            kryo.register(java.util.ArrayList.class, 10);
            kryo.register(java.util.HashMap.class, 11);
            kryo.register(java.util.LinkedHashMap.class, 12);
            kryo.register(byte[].class, 13);

            kryo.register(NodeAddress.class, 20);
            kryo.register(LogEntry.class, 21);
            kryo.register(AppendEntriesRequest.class, 22);
            kryo.register(AppendEntriesResponse.class, 23);
            kryo.register(RequestVoteRequest.class, 24);
            kryo.register(RequestVoteResponse.class, 25);
            kryo.register(ClientCommandRequest.class, 26);
            kryo.register(InstallSnapshotRequest.class, 27);
            kryo.register(InstallSnapshotResponse.class, 28);

            int customIdBase = 100;
            for (Class<?> clazz : customClasses) {
                kryo.register(clazz, customIdBase++);
            }

            return kryo;
        });
    }

    public void serialize(Object obj, OutputStream outStream) {
        if (obj == null) return;
        Kryo kryo = KRYO_THREAD_LOCAL.get();
        Output output = new Output(outStream);
        kryo.writeClassAndObject(output, obj);
        output.flush();
    }

    public Object deserialize(InputStream inStream) {
        Kryo kryo = KRYO_THREAD_LOCAL.get();
        Input input = new Input(inStream);
        return kryo.readClassAndObject(input);
    }
}
