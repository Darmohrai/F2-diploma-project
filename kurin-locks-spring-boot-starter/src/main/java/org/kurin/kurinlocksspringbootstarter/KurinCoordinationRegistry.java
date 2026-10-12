package org.kurin.kurinlocksspringbootstarter;

import org.kurin.api.LeadershipToken;
import org.kurin.kurinlocks.leader.CancellationSignal;
import org.kurin.kurinlocksspringbootstarter.annotation.KurinDistributedLock;
import org.kurin.kurinlocksspringbootstarter.annotation.KurinLeaderOnly;
import org.kurin.kurinlocksspringbootstarter.annotation.KurinLeaderTask;
import org.kurin.kurinlocksspringbootstarter.annotation.KurinScheduledOnce;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.convert.DurationStyle;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.expression.ParseException;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Checks the Kurin coordination annotations of every bean when the bean is created, so that a misuse stops the
 * application at startup instead of surfacing on the first call, and collects the {@code @KurinLeaderTask}
 * methods for registration once the node is running.
 */
public class KurinCoordinationRegistry implements BeanPostProcessor {

    public record LeaderTaskMethod(String beanName, Method method, String name, Duration interval) {
    }

    private final SpelExpressionParser parser = new SpelExpressionParser();
    private final List<LeaderTaskMethod> leaderTasks = new ArrayList<>();
    private final Map<String, String> leaderTaskOwners = new HashMap<>();

    @Override
    public synchronized Object postProcessAfterInitialization(Object bean, String beanName) {
        Class<?> type = AopUtils.getTargetClass(bean);
        if (type.getName().startsWith("org.springframework.") || type.getName().startsWith("java.")) {
            return bean;
        }
        try {
            ReflectionUtils.doWithMethods(type, method -> validate(beanName, method), method -> !method.isBridge() && !method.isSynthetic());
        } catch (LinkageError e) {
            // The class refers to optional dependencies that are absent, so it cannot carry our annotations either
            return bean;
        }
        return bean;
    }

    private void validate(String beanName, Method method) {
        String where = method.getDeclaringClass().getName() + "." + method.getName();

        KurinDistributedLock lock = AnnotatedElementUtils.findMergedAnnotation(method, KurinDistributedLock.class);
        if (lock != null) {
            if (lock.key().isBlank()) {
                throw new IllegalStateException("@KurinDistributedLock on " + where + " needs a key expression");
            }
            try {
                parser.parseExpression(lock.key());
            } catch (ParseException e) {
                throw new IllegalStateException("@KurinDistributedLock on " + where + " has an invalid key expression '"
                        + lock.key() + "': " + e.getMessage() + ". A constant key must be quoted, for example \"'my-lock'\".", e);
            }
            Duration ttl = duration(lock.ttl(), "ttl", "@KurinDistributedLock", where);
            if (ttl.isZero() || ttl.isNegative()) {
                throw new IllegalStateException("@KurinDistributedLock on " + where + " needs a positive ttl");
            }
            Duration waitTimeout = duration(lock.waitTimeout(), "waitTimeout", "@KurinDistributedLock", where);
            if (lock.onBusy() == KurinDistributedLock.OnBusy.WAIT && (waitTimeout.isZero() || waitTimeout.isNegative())) {
                throw new IllegalStateException("@KurinDistributedLock(onBusy = WAIT) on " + where + " needs a positive waitTimeout");
            }
            if (lock.onBusy() == KurinDistributedLock.OnBusy.SKIP) {
                rejectPrimitiveResult(method, "@KurinDistributedLock(onBusy = SKIP)", where);
            }
        }

        KurinLeaderOnly leaderOnly = AnnotatedElementUtils.findMergedAnnotation(method, KurinLeaderOnly.class);
        if (leaderOnly != null && leaderOnly.onNotLeader() == KurinLeaderOnly.OnNotLeader.SKIP) {
            rejectPrimitiveResult(method, "@KurinLeaderOnly(onNotLeader = SKIP)", where);
        }

        KurinScheduledOnce once = AnnotatedElementUtils.findMergedAnnotation(method, KurinScheduledOnce.class);
        if (once != null) {
            if (once.name().isBlank()) {
                throw new IllegalStateException("@KurinScheduledOnce on " + where + " needs a job name");
            }
            if (duration(once.slot(), "slot", "@KurinScheduledOnce", where).toMillis() <= 0
                    || duration(once.lease(), "lease", "@KurinScheduledOnce", where).toMillis() <= 0) {
                throw new IllegalStateException("@KurinScheduledOnce on " + where + " needs a slot and a lease of at least one millisecond");
            }
            rejectPrimitiveResult(method, "@KurinScheduledOnce", where);
        }

        KurinLeaderTask task = AnnotatedElementUtils.findMergedAnnotation(method, KurinLeaderTask.class);
        if (task != null) {
            if (task.name().isBlank()) {
                throw new IllegalStateException("@KurinLeaderTask on " + where + " needs a name");
            }
            Duration interval = duration(task.interval(), "interval", "@KurinLeaderTask", where);
            if (interval.toMillis() <= 0) {
                throw new IllegalStateException("@KurinLeaderTask on " + where + " needs an interval of at least one millisecond");
            }
            Class<?>[] parameters = method.getParameterTypes();
            boolean supported = parameters.length == 0
                    || (parameters.length == 1 && parameters[0] == LeadershipToken.class)
                    || (parameters.length == 2 && parameters[0] == LeadershipToken.class && parameters[1] == CancellationSignal.class);
            if (!supported) {
                throw new IllegalStateException("@KurinLeaderTask method " + where + " must take no parameters, "
                        + "(LeadershipToken) or (LeadershipToken, CancellationSignal)");
            }
            String previous = leaderTaskOwners.putIfAbsent(task.name(), where);
            if (previous != null && !previous.equals(where)) {
                throw new IllegalStateException("Leader task name '" + task.name() + "' is used by both " + previous + " and " + where);
            }
            if (previous == null) {
                leaderTasks.add(new LeaderTaskMethod(beanName, method, task.name(), interval));
            }
        }
    }

    private static void rejectPrimitiveResult(Method method, String annotation, String where) {
        Class<?> returnType = method.getReturnType();
        if (returnType.isPrimitive() && returnType != void.class) {
            throw new IllegalStateException(annotation + " cannot be used on " + where + ": it returns the primitive type '"
                    + returnType.getName() + "', so there is no value to return when the method is skipped. "
                    + "Return void, a wrapper type or Optional, or choose THROW.");
        }
    }

    private static Duration duration(String value, String attribute, String annotation, String where) {
        try {
            return DurationStyle.detectAndParse(value);
        } catch (RuntimeException e) {
            throw new IllegalStateException(annotation + " on " + where + " has an invalid " + attribute + " '" + value
                    + "'. Use an ISO-8601 duration such as PT30S or a simple one such as 30s.", e);
        }
    }

    public static Duration parseDuration(String value) {
        return DurationStyle.detectAndParse(value);
    }

    public synchronized List<LeaderTaskMethod> leaderTasks() {
        return List.copyOf(leaderTasks);
    }
}
