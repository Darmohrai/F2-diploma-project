package org.kurin.routing.spring;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.kurin.routing.KurinPrimary;
import org.kurin.routing.KurinReadOnly;
import org.kurin.routing.KurinRoutingContext;
import org.kurin.routing.KurinRoutingException;
import org.kurin.routing.KurinShardKey;
import org.kurin.routing.KurinSharded;
import org.kurin.routing.shard.KurinShardRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.context.expression.MethodBasedEvaluationContext;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.Ordered;
import org.springframework.core.ParameterNameDiscoverer;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.annotation.Order;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Turns the routing annotations into the routing context of the calling thread, for the duration of the call.
 * It runs before the transaction advice, so the context is in place when the transaction asks for its connection.
 */
@Aspect
@Order(Ordered.HIGHEST_PRECEDENCE + 100)
public class KurinRoutingAspect {
    private static final Logger log = LoggerFactory.getLogger(KurinRoutingAspect.class);

    private final KurinShardRouter shardRouter;
    private final SpelExpressionParser parser = new SpelExpressionParser();
    private final ParameterNameDiscoverer parameterNames = new DefaultParameterNameDiscoverer();
    private final Map<Method, Integer> shardKeyParameterIndexes = new ConcurrentHashMap<>();
    private final Map<Method, Expression> shardKeyExpressions = new ConcurrentHashMap<>();
    private final Map<Method, KurinReadOnly.Consistency> readConsistencies = new ConcurrentHashMap<>();

    public KurinRoutingAspect(KurinShardRouter shardRouter) {
        this.shardRouter = shardRouter;
    }

    @Around("@annotation(org.kurin.routing.KurinReadOnly) || @within(org.kurin.routing.KurinReadOnly)")
    public Object routeRead(ProceedingJoinPoint joinPoint) throws Throwable {
        Method method = targetMethod(joinPoint);
        KurinReadOnly.Consistency consistency = readConsistencies.computeIfAbsent(method, m -> {
            // The annotation of the method wins over the one of its class
            KurinReadOnly readOnly = AnnotatedElementUtils.findMergedAnnotation(m, KurinReadOnly.class);
            if (readOnly == null) {
                readOnly = AnnotatedElementUtils.findMergedAnnotation(m.getDeclaringClass(), KurinReadOnly.class);
            }
            return readOnly != null ? readOnly.consistency() : KurinReadOnly.Consistency.READ_YOUR_WRITES;
        });
        try (KurinRoutingContext.Scope ignored = KurinRoutingContext.readOnly(consistency)) {
            return joinPoint.proceed();
        }
    }

    @Around("@annotation(org.kurin.routing.KurinPrimary) || @within(org.kurin.routing.KurinPrimary)")
    public Object routePrimary(ProceedingJoinPoint joinPoint) throws Throwable {
        try (KurinRoutingContext.Scope ignored = KurinRoutingContext.primary()) {
            return joinPoint.proceed();
        }
    }

    @Around("@annotation(sharded)")
    public Object routeSharded(ProceedingJoinPoint joinPoint, KurinSharded sharded) throws Throwable {
        Method method = targetMethod(joinPoint);
        Object key;
        try {
            key = shardKeyExpressions.computeIfAbsent(method, m -> parser.parseExpression(sharded.key()))
                    .getValue(new MethodBasedEvaluationContext(joinPoint.getTarget(), method, joinPoint.getArgs(), parameterNames));
        } catch (RuntimeException e) {
            throw new KurinRoutingException("Cannot evaluate shard key expression '" + sharded.key() + "' of "
                    + method.getDeclaringClass().getSimpleName() + "." + method.getName() + ": " + e.getMessage(), e);
        }
        if (key == null) {
            throw new KurinRoutingException("Shard key expression '" + sharded.key() + "' of "
                    + method.getDeclaringClass().getSimpleName() + "." + method.getName() + " evaluated to null");
        }
        return proceedOnShard(joinPoint, method, shardRouter.route(key));
    }

    @Around("execution(* *(.., @org.kurin.routing.KurinShardKey (*), ..))")
    public Object routeShardKeyParameter(ProceedingJoinPoint joinPoint) throws Throwable {
        Method method = targetMethod(joinPoint);
        int index = shardKeyParameterIndexes.computeIfAbsent(method, KurinRoutingAspect::findShardKeyParameter);
        if (index < 0) {
            // The annotation sits on the interface method only
            Method declared = ((MethodSignature) joinPoint.getSignature()).getMethod();
            index = shardKeyParameterIndexes.computeIfAbsent(declared, KurinRoutingAspect::findShardKeyParameter);
        }
        if (index < 0) {
            return joinPoint.proceed();
        }
        return proceedOnShard(joinPoint, method, shardRouter.route(joinPoint.getArgs()[index]));
    }

    private Object proceedOnShard(ProceedingJoinPoint joinPoint, Method method, String shard) throws Throwable {
        log.debug("{} routed to shard '{}'", method.getName(), shard);
        // Inside a running transaction this is the moment a second shard is noticed
        ShardTransactionGuard.enter(shard);
        try (KurinRoutingContext.Scope ignored = KurinRoutingContext.shard(shard)) {
            return joinPoint.proceed();
        }
    }

    private static Method targetMethod(ProceedingJoinPoint joinPoint) {
        Method method = ((MethodSignature) joinPoint.getSignature()).getMethod();
        return joinPoint.getTarget() != null ? AopUtils.getMostSpecificMethod(method, joinPoint.getTarget().getClass()) : method;
    }

    private static int findShardKeyParameter(Method method) {
        Annotation[][] annotations = method.getParameterAnnotations();
        for (int i = 0; i < annotations.length; i++) {
            for (Annotation annotation : annotations[i]) {
                if (annotation instanceof KurinShardKey) {
                    return i;
                }
            }
        }
        return -1;
    }
}
