package org.kurin.network.serializer;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import com.esotericsoftware.kryo.kryo5.objenesis.strategy.StdInstantiatorStrategy;
import com.esotericsoftware.kryo.kryo5.util.DefaultInstantiatorStrategy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class KryoSerializer {

    private final ThreadLocal<Kryo> KRYO_THREAD_LOCAL = ThreadLocal.withInitial(() -> {
        Kryo kryo = new Kryo();

        kryo.setRegistrationRequired(false);
        kryo.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));

        return kryo;
    });

    public byte[] serialize(Object obj) {
        if (obj == null) {
            return new byte[0];
        }

        Kryo kryo = KRYO_THREAD_LOCAL.get();

        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
             Output output = new Output(outputStream)) {

            kryo.writeClassAndObject(output, obj);
            output.flush();

            return outputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Object deserialize(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }

        Kryo kryo = KRYO_THREAD_LOCAL.get();

        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
             Input input = new Input(inputStream)) {

            return kryo.readClassAndObject(input);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
