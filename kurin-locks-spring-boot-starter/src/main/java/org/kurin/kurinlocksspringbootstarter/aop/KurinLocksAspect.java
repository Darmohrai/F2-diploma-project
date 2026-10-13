package org.kurin.kurinlocksspringbootstarter.aop;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.kurin.api.LeadershipToken;
import org.kurin.api.exception.KurinNotLeaderException;
import org.kurin.kurinlocks.api.KurinLockContext;
import org.kurin.kurinlocks.api.KurinLockManager;
import org.kurin.kurinlocks.api.LockHandle;
import org.kurin.kurinlocks.api.LockOptions;
import org.kurin.kurinlocks.exception.KurinLockAcquisitionException;
import org.kurin.kurinlocks.job.KurinJobs;
import org.kurin.kurinlocks.leader.KurinLeadership;
import org.kurin.kurinlocksspringbootstarter.KurinCoordinationRegistry;
import org.kurin.kurinlocksspringbootstarter.KurinLockKeyException;
import org.kurin.kurinlocksspringbootstarter.annotation.KurinDistributedLock;
import org.kurin.kurinlocksspringbootstarter.annotation.KurinLeaderOnly;
import org.kurin.kurinlocksspringbootstarter.annotation.KurinScheduledOnce;
import org.kurin.raft.KurinNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.context.expression.MethodBasedEvaluationContext;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.Ordered;
import org.springframework.core.ParameterNameDiscoverer;
import org.springframework.core.annotation.Order;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Applies {@code @KurinDistributedLock}, {@code @KurinLeaderOnly} and {@code @KurinScheduledOnce}.
 * It runs outside of the transaction advice: a lock is taken before the transaction begins and released
 * after it has committed.
 */
@Aspect
@Order(Ordered.HIGHEST_PRECEDENCE + 200)
public class KurinLocksAspect {
    private static final Logger log = LoggerFactory.getLogger(KurinLocksAspect.class);

    /**
     * Root object of key expressions: {@code #root.methodName}, {@code #root.target}.
     */
    public record KeyRoot(String methodName, Object target, Object[] args) {
    }

    private record LockPlan(Expression key, LockOptions options, KurinDistributedLock.OnBusy onBusy, String description) {
    }

    private record JobPlan(String name, Duration slot, Duration lease) {
    }

    private final KurinLockManager lockManager;
    private final KurinJobs jobs;
    private final KurinNode kurinNode;
    private final SpelExpressionParser parser = new SpelExpressionParser();
    private final ParameterNameDiscoverer parameterNames = new DefaultParameterNameDiscoverer();
    private final Map<Method, LockPlan> lockPlans = new ConcurrentHashMap<>();
    private final Map<Method, JobPlan> jobPlans = new ConcurrentHashMap<>();

    public KurinLocksAspect(KurinLockManager lockManager, KurinJobs jobs, KurinNode kurinNode) {
        this.lockManager = lockManager;
        this.jobs = jobs;
        this.kurinNode = kurinNode;
    }

    // ---------------------------------------------------------------- @KurinDistributedLock

    @Around("@annotation(kurinLock)")
    public Object handleDistributedLock(ProceedingJoinPoint joinPoint, KurinDistributedLock kurinLock) throws Throwable {
        Method method = targetMethod(joinPoint);
        // The annotation attributes and the parsed key expression are resolved once per method
        LockPlan plan = lockPlans.computeIfAbsent(method, m -> lockPlan(m, kurinLock));
        String key = evaluateKey(plan, method, joinPoint);

        // Empty means busy and nothing else. When the cluster gave no definite answer, tryLock throws the
        // KurinException that says why, and it leaves this advice whatever onBusy is: the method does not run.
        Optional<LockHandle> acquired = lockManager.tryLock(key, plan.options());
        if (acquired.isEmpty()) {
            if (plan.onBusy() == KurinDistributedLock.OnBusy.SKIP) {
                log.debug("Lock '{}' is busy: skipping {}", key, plan.description());
                return emptyResult(method);
            }
            throw new KurinLockAcquisitionException("Lock '" + key + "' is held by another owner; " + plan.description()
                    + " was not executed" + (plan.options().waitTimeout().isZero() ? ""
                    : " after waiting " + plan.options().waitTimeout().toMillis() + " ms"));
        }

        LockHandle handle = acquired.get();
        KurinLockContext.push(handle);
        try {
            return joinPoint.proceed();
        } finally {
            KurinLockContext.pop();
            handle.release();
        }
    }

    private LockPlan lockPlan(Method method, KurinDistributedLock annotation) {
        LockOptions options = LockOptions.ttl(KurinCoordinationRegistry.parseDuration(annotation.ttl()))
                .waitTimeout(KurinCoordinationRegistry.parseDuration(annotation.waitTimeout()))
                .autoRenew(annotation.autoRenew())
                .reentrant(annotation.reentrant());
        return new LockPlan(parser.parseExpression(annotation.key()), options, annotation.onBusy(),
                method.getDeclaringClass().getSimpleName() + "." + method.getName());
    }

    private String evaluateKey(LockPlan plan, Method method, ProceedingJoinPoint joinPoint) {
        Object value;
        try {
            KeyRoot root = new KeyRoot(method.getName(), joinPoint.getTarget(), joinPoint.getArgs());
            value = plan.key().getValue(new MethodBasedEvaluationContext(root, method, joinPoint.getArgs(), parameterNames));
        } catch (RuntimeException e) {
            throw new KurinLockKeyException("Cannot evaluate lock key expression '" + plan.key().getExpressionString()
                    + "' of " + plan.description() + ": " + e.getMessage(), e);
        }
        if (value == null) {
            throw new KurinLockKeyException("Lock key expression '" + plan.key().getExpressionString() + "' of "
                    + plan.description() + " evaluated to null. If it refers to a parameter by name, compile with "
                    + "-parameters or use #a0, #a1, ...");
        }
        return value.toString();
    }

    // ---------------------------------------------------------------- @KurinLeaderOnly

    @Around("@annotation(leaderOnly)")
    public Object handleLeaderOnly(ProceedingJoinPoint joinPoint, KurinLeaderOnly leaderOnly) throws Throwable {
        Optional<LeadershipToken> token = kurinNode.leadership();
        if (token.isEmpty()) {
            Method method = targetMethod(joinPoint);
            if (leaderOnly.onNotLeader() == KurinLeaderOnly.OnNotLeader.SKIP) {
                log.trace("Node {} is not the established leader: skipping {}", kurinNode.nodeId(), method.getName());
                return emptyResult(method);
            }
            throw new KurinNotLeaderException("Node " + kurinNode.nodeId() + " is not the established leader; "
                    + method.getDeclaringClass().getSimpleName() + "." + method.getName() + " was not executed",
                    kurinNode.status().leaderId());
        }
        return proceedUnwrapping(() -> KurinLeadership.callWith(token.get(), () -> proceedWrapping(joinPoint)));
    }

    // ---------------------------------------------------------------- @KurinScheduledOnce

    @Around("@annotation(scheduledOnce)")
    public Object handleScheduledOnce(ProceedingJoinPoint joinPoint, KurinScheduledOnce scheduledOnce) throws Throwable {
        Method method = targetMethod(joinPoint);
        JobPlan plan = jobPlans.computeIfAbsent(method, m -> new JobPlan(scheduledOnce.name(),
                KurinCoordinationRegistry.parseDuration(scheduledOnce.slot()),
                KurinCoordinationRegistry.parseDuration(scheduledOnce.lease())));

        Optional<Object> result = proceedUnwrapping(() ->
                jobs.runOnce(plan.name(), plan.slot(), scheduledOnce.mode(), plan.lease(), () -> proceedWrapping(joinPoint)));
        return result.isPresent() ? result.get() : emptyResult(method);
    }

    // ---------------------------------------------------------------- helpers

    private static Method targetMethod(ProceedingJoinPoint joinPoint) {
        Method method = ((MethodSignature) joinPoint.getSignature()).getMethod();
        return joinPoint.getTarget() != null ? AopUtils.getMostSpecificMethod(method, joinPoint.getTarget().getClass()) : method;
    }

    private static Object emptyResult(Method method) {
        return method.getReturnType() == Optional.class ? Optional.empty() : null;
    }

    // Checked Throwables of the advised method travel through Callable-based APIs wrapped, and are unwrapped again
    private static Object proceedWrapping(ProceedingJoinPoint joinPoint) throws Exception {
        try {
            return joinPoint.proceed();
        } catch (Exception | Error e) {
            throw e;
        } catch (Throwable t) {
            throw new UndeclaredThrowableException(t);
        }
    }

    private interface ThrowingSupplier<T> {
        T get() throws Exception;
    }

    private static <T> T proceedUnwrapping(ThrowingSupplier<T> action) throws Throwable {
        try {
            return action.get();
        } catch (UndeclaredThrowableException e) {
            throw e.getUndeclaredThrowable();
        }
    }
}
