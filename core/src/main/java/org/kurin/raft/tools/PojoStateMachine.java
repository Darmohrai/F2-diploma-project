package org.kurin.raft.tools;

import org.kurin.api.KurinCommand;
import org.kurin.api.KurinQuery;
import org.kurin.api.KurinRestore;
import org.kurin.api.KurinSnapshot;
import org.kurin.network.serializer.KryoSerializer;
import org.kurin.raft.state.StateMachine;
import org.kurin.raft.state.StateMachineListener;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PojoStateMachine implements StateMachine {

    private record Handler(Object instance, Method method) {
    }

    private final Map<Class<?>, Handler> commandHandlers = new HashMap<>();
    private final Map<Class<?>, Handler> queryHandlers = new HashMap<>();
    private final List<Handler> snapshotHandlers = new ArrayList<>();
    private final List<Handler> restoreHandlers = new ArrayList<>();
    private final List<StateMachineListener> listeners = new ArrayList<>();
    private final KryoSerializer serializer;

    public PojoStateMachine(List<Object> services, KryoSerializer serializer) {
        this.serializer = serializer;
        for (Object service : services) {
            Class<?> clazz = service.getClass();
            if (clazz.getName().contains("$$")) {
                clazz = clazz.getSuperclass();
            }

            for (Method method : clazz.getDeclaredMethods()) {
                method.setAccessible(true);
                if (method.isAnnotationPresent(KurinCommand.class)) {
                    if (method.getParameterCount() != 1) {
                        throw new IllegalArgumentException("@KurinCommand must have 1 param: " + method.getName());
                    }
                    Class<?> commandType = method.getParameterTypes()[0];
                    if (commandHandlers.containsKey(commandType)) {
                        throw new IllegalStateException("Routing conflict for command: " + commandType.getSimpleName());
                    }
                    commandHandlers.put(commandType, new Handler(service, method));

                } else if (method.isAnnotationPresent(KurinQuery.class)) {
                    if (method.getParameterCount() != 1) {
                        throw new IllegalArgumentException("@KurinQuery must have 1 param: " + method.getName());
                    }
                    Class<?> queryType = method.getParameterTypes()[0];
                    if (queryHandlers.containsKey(queryType)) {
                        throw new IllegalStateException("Routing conflict for query: " + queryType.getSimpleName());
                    }
                    queryHandlers.put(queryType, new Handler(service, method));

                } else if (method.isAnnotationPresent(KurinSnapshot.class)) {
                    snapshotHandlers.add(new Handler(service, method));
                } else if (method.isAnnotationPresent(KurinRestore.class)) {
                    restoreHandlers.add(new Handler(service, method));
                }
            }
        }
    }

    @Override
    public Object apply(Object command) {
        Handler handler = commandHandlers.get(command.getClass());
        if (handler == null) {
            throw new IllegalArgumentException("No handler found for command: " + command.getClass());
        }
        try {
            Object result = handler.method().invoke(handler.instance(), command);
            for (StateMachineListener listener : listeners) {
                listener.onCommandApplied(command, result);
            }
            return result;
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Kurin command execution failed", e);
        }
    }

    @Override
    public Object executeQuery(Object query) {
        Handler handler = queryHandlers.get(query.getClass());
        if (handler == null) {
            throw new IllegalArgumentException("No handler found for query: " + query.getClass());
        }
        try {
            return handler.method().invoke(handler.instance(), query);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Kurin query execution failed", e);
        }
    }

    @Override
    public byte[] takeSnapshot() {
        Map<String, byte[]> aggregateState = new HashMap<>();
        try {
            for (Handler handler : snapshotHandlers) {
                byte[] data = (byte[]) handler.method().invoke(handler.instance());
                aggregateState.put(handler.instance().getClass().getName(), data);
            }
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            serializer.serialize(aggregateState, bos);
            return bos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Failed to aggregate snapshots", e);
        }
    }

    @Override
    public void installSnapshot(byte[] data) {
        if (data == null || data.length == 0) return;
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(data);
            @SuppressWarnings("unchecked")
            Map<String, byte[]> aggregateState = (Map<String, byte[]>) serializer.deserialize(bis);
            for (Handler handler : restoreHandlers) {
                String serviceName = handler.instance().getClass().getName();
                byte[] serviceData = aggregateState.get(serviceName);
                if (serviceData != null) {
                    handler.method().invoke(handler.instance(), serviceData);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to distribute snapshot", e);
        }
    }

    @Override
    public void addListener(StateMachineListener listener) {
        this.listeners.add(listener);
    }
}
