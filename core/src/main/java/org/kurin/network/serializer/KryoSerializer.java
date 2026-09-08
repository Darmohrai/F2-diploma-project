package org.kurin.network.serializer;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import com.esotericsoftware.kryo.kryo5.objenesis.strategy.StdInstantiatorStrategy;
import com.esotericsoftware.kryo.kryo5.util.DefaultInstantiatorStrategy;

import java.io.*;

public class KryoSerializer {

    private final ThreadLocal<Kryo> KRYO_THREAD_LOCAL = ThreadLocal.withInitial(() -> {
        Kryo kryo = new Kryo();

        kryo.setRegistrationRequired(false);
        kryo.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));

        return kryo;
    });

    public void serialize(Object obj, OutputStream outStream) {
        if (obj == null) {
            return;
        }

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
