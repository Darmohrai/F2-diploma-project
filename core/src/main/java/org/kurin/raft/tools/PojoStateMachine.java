package org.kurin.raft.tools;

import org.kurin.api.KurinCommand;
import org.kurin.api.KurinQuery;
import org.kurin.api.KurinRestore;
import org.kurin.api.KurinService;
import org.kurin.api.KurinSnapshot;
import org.kurin.network.serializer.KryoSerializer;
import org.kurin.raft.state.StateMachine;
import org.kurin.raft.state.StateMachineListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * State machine assembled from plain objects: commands and queries are routed to the method whose single
 * parameter has the type of the message.
 */
public class PojoStateMachine implements StateMachine {
    private static final Logger log = LoggerFactory.getLogger(PojoStateMachine.class);

    /**
     * A service object together with the class that declares its annotated methods. They differ when the
     * object is a proxy of the real service.
     */
    public record Service(Object instance, Class<?> type) {
        public static Service of(Object instance) {
            Class<?> type = instance.getClass();
            // Generated subclasses (CGLIB and similar) carry no annotations of their own
            while (type.getName().contains("$$") && type.getSuperclass() != null && type.getSuperclass() != Object.class) {
                type = type.getSuperclass();
            }
            return new Service(instance, type);
        }
    }

    private record Handler(Object instance, Method method) {
    }

    private final Map<Class<?>, Handler> commandHandlers = new HashMap<>();
    private final Map<Class<?>, Handler> queryHandlers = new HashMap<>();
    private final Map<String, Handler> snapshotHandlers = new LinkedHashMap<>();
    private final Map<String, Handler> restoreHandlers = new LinkedHashMap<>();
    private final List<Method> messageHandlerMethods = new ArrayList<>();
    private final List<StateMachineListener> listeners = new ArrayList<>();
    private KryoSerializer serializer;

    /**
     * @throws IllegalStateException if a handler has an unsupported signature or two handlers accept the same type
     */
    public PojoStateMachine(List<Service> services) {
        Map<String, Class<?>> serviceIds = new HashMap<>();
        for (Service service : services) {
            String serviceId = serviceId(service.type());
            Class<?> clash = serviceIds.put(serviceId, service.type());
            if (clash != null && clash != service.type()) {
                throw new IllegalStateException("Service id '" + serviceId + "' is used by both " + clash.getName()
                        + " and " + service.type().getName() + ". Give them distinct @KurinService names.");
            }
            for (Method method : annotatedMethods(service.type())) {
                register(service, serviceId, method);
            }
        }
    }

    private void register(Service service, String serviceId, Method method) {
        String where = method.getDeclaringClass().getName() + "." + method.getName();
        method.setAccessible(true);
        Handler handler = new Handler(service.instance(), method);

        if (method.isAnnotationPresent(KurinCommand.class) || method.isAnnotationPresent(KurinQuery.class)) {
            boolean command = method.isAnnotationPresent(KurinCommand.class);
            String annotation = command ? "@KurinCommand" : "@KurinQuery";
            if (method.getParameterCount() != 1) {
                throw new IllegalStateException(annotation + " method " + where + " must have exactly one parameter, the "
                        + (command ? "command" : "query") + " object");
            }
            Class<?> messageType = method.getParameterTypes()[0];
            Handler previous = (command ? commandHandlers : queryHandlers).putIfAbsent(messageType, handler);
            if (previous != null) {
                throw new IllegalStateException("Two " + annotation + " methods accept " + messageType.getName() + ": "
                        + previous.method().getDeclaringClass().getName() + "." + previous.method().getName() + " and " + where);
            }
            messageHandlerMethods.add(method);
        } else if (method.isAnnotationPresent(KurinSnapshot.class)) {
            if (method.getParameterCount() != 0 || method.getReturnType() != byte[].class) {
                throw new IllegalStateException("@KurinSnapshot method " + where + " must take no parameters and return byte[]");
            }
            if (snapshotHandlers.putIfAbsent(serviceId, handler) != null) {
                throw new IllegalStateException("Service " + serviceId + " declares more than one @KurinSnapshot method");
            }
        } else if (method.isAnnotationPresent(KurinRestore.class)) {
            if (method.getParameterCount() != 1 || method.getParameterTypes()[0] != byte[].class) {
                throw new IllegalStateException("@KurinRestore method " + where + " must take a single byte[] parameter");
            }
            if (restoreHandlers.putIfAbsent(serviceId, handler) != null) {
                throw new IllegalStateException("Service " + serviceId + " declares more than one @KurinRestore method");
            }
        }
    }

    /**
     * @return true if the class declares or inherits at least one Kurin handler method
     */
    public static boolean isService(Class<?> type) {
        return !annotatedMethods(type).isEmpty();
    }

    // Walks the hierarchy from the top, so that an overriding method replaces the inherited one
    private static List<Method> annotatedMethods(Class<?> type) {
        List<Class<?>> hierarchy = new ArrayList<>();
        for (Class<?> current = type; current != null && current != Object.class; current = current.getSuperclass()) {
            hierarchy.add(0, current);
        }
        Map<String, Method> bySignature = new LinkedHashMap<>();
        for (Class<?> current : hierarchy) {
            for (Method method : current.getDeclaredMethods()) {
                if (method.isBridge() || method.isSynthetic()) {
                    continue;
                }
                String signature = method.getName() + java.util.Arrays.toString(method.getParameterTypes());
                if (isHandler(method)) {
                    bySignature.put(signature, method);
                } else {
                    Method inherited = bySignature.get(signature);
                    if (inherited != null) {
                        // An override without annotations keeps the role of the method it overrides
                        bySignature.put(signature, inherited);
                    }
                }
            }
        }
        return new ArrayList<>(bySignature.values());
    }

    private static boolean isHandler(Method method) {
        return method.isAnnotationPresent(KurinCommand.class) || method.isAnnotationPresent(KurinQuery.class)
                || method.isAnnotationPresent(KurinSnapshot.class) || method.isAnnotationPresent(KurinRestore.class);
    }

    private static String serviceId(Class<?> type) {
        KurinService named = type.getAnnotation(KurinService.class);
        return named != null ? named.value() : type.getName();
    }

    /**
     * @return the command and query handler methods, whose parameter and result types travel over the wire
     */
    public List<Method> messageHandlerMethods() {
        return List.copyOf(messageHandlerMethods);
    }

    /**
     * Supplies the node serializer, used to pack the snapshots of all services into one blob.
     */
    public void bindSerializer(KryoSerializer serializer) {
        this.serializer = serializer;
    }

    @Override
    public Object apply(Object command) {
        Handler handler = commandHandlers.get(command.getClass());
        if (handler == null) {
            throw new IllegalArgumentException("No @KurinCommand handler is registered for " + command.getClass().getName());
        }
        Object result = invoke(handler, command);
        for (StateMachineListener listener : listeners) {
            try {
                listener.onCommandApplied(command, result);
            } catch (RuntimeException e) {
                // A listener is a side channel: it must not change the result or stop later entries
                log.warn("State machine listener {} failed for {}", listener.getClass().getName(),
                        command.getClass().getSimpleName(), e);
            }
        }
        return result;
    }

    @Override
    public Object executeQuery(Object query) {
        Handler handler = queryHandlers.get(query.getClass());
        if (handler == null) {
            throw new IllegalArgumentException("No @KurinQuery handler is registered for " + query.getClass().getName());
        }
        return invoke(handler, query);
    }

    private static Object invoke(Handler handler, Object... arguments) {
        try {
            return handler.method().invoke(handler.instance(), arguments);
        } catch (InvocationTargetException e) {
            throw new HandlerFailedException(e.getCause());
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Cannot call " + handler.method(), e);
        }
    }

    @Override
    public byte[] takeSnapshot() {
        Map<String, byte[]> aggregate = new LinkedHashMap<>();
        snapshotHandlers.forEach((serviceId, handler) -> aggregate.put(serviceId, (byte[]) invoke(handler)));
        return serializer.toBytes(aggregate);
    }

    @Override
    public void installSnapshot(byte[] data) {
        if (data == null || data.length == 0) {
            return;
        }
        @SuppressWarnings("unchecked")
        Map<String, byte[]> aggregate = (Map<String, byte[]>) serializer.fromBytes(data);
        restoreHandlers.forEach((serviceId, handler) -> {
            byte[] serviceData = aggregate.get(serviceId);
            if (serviceData != null) {
                invoke(handler, (Object) serviceData);
            }
        });
    }

    @Override
    public void addListener(StateMachineListener listener) {
        this.listeners.add(listener);
    }
}
