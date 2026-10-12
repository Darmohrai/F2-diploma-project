package org.kurin.kurincachespringbootstarter;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.kurin.kurintieredcache.api.DataSourceType;
import org.kurin.kurintieredcache.api.KurinRoutingContext;
import org.kurin.kurintieredcache.api.KurinShardKey;
import org.kurin.kurintieredcache.routing.KurinShardRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

// Must run before the transaction interceptor: the connection is picked when the transaction starts
@Aspect
@Order(Ordered.HIGHEST_PRECEDENCE + 100)
public class KurinRoutingAspect {
    private static final Logger log = LoggerFactory.getLogger(KurinRoutingAspect.class);

    private final SessionStickyManager stickyManager;
    private final KurinClientIdResolver clientIdResolver;
    private final KurinShardRouter shardRouter;
    private final Map<Method, Integer> shardKeyParameterIndexes = new ConcurrentHashMap<>();

    public KurinRoutingAspect(SessionStickyManager stickyManager,
                              KurinClientIdResolver clientIdResolver,
                              KurinShardRouter shardRouter) {
        this.stickyManager = stickyManager;
        this.clientIdResolver = clientIdResolver;
        this.shardRouter = shardRouter;
    }

    @Around("@annotation(org.kurin.kurintieredcache.api.KurinReadOnly) || @within(org.kurin.kurintieredcache.api.KurinReadOnly)")
    public Object routeRead(ProceedingJoinPoint joinPoint) throws Throwable {
        String clientId = clientIdResolver.resolveClientId();
        DataSourceType previous = KurinRoutingContext.peek();

        if (stickyManager.shouldRouteToPrimary(clientId)) {
            log.debug("[Routing] Session Sticky active for client {}. Routing to PRIMARY to avoid Stale Read.", clientId);
            KurinRoutingContext.set(DataSourceType.PRIMARY);
        } else {
            log.debug("[Routing] Routing read request to REPLICA.");
            KurinRoutingContext.set(DataSourceType.REPLICA);
        }

        try {
            return joinPoint.proceed();
        } finally {
            if (previous != null) {
                KurinRoutingContext.set(previous);
            } else {
                KurinRoutingContext.clear();
            }
        }
    }

    @Around("execution(* *(.., @org.kurin.kurintieredcache.api.KurinShardKey (*), ..))")
    public Object routeShard(ProceedingJoinPoint joinPoint) throws Throwable {
        Method method = ((MethodSignature) joinPoint.getSignature()).getMethod();
        Class<?> targetClass = joinPoint.getTarget() != null ? joinPoint.getTarget().getClass() : method.getDeclaringClass();
        int index = shardKeyParameterIndexes.computeIfAbsent(
                AopUtils.getMostSpecificMethod(method, targetClass), KurinRoutingAspect::findShardKeyParameter);
        if (index < 0) {
            index = shardKeyParameterIndexes.computeIfAbsent(method, KurinRoutingAspect::findShardKeyParameter);
        }
        if (index < 0) {
            return joinPoint.proceed();
        }

        String shard = shardRouter.route(joinPoint.getArgs()[index]);
        log.debug("[Routing] Method {} routed to shard '{}'.", method.getName(), shard);

        String previous = KurinRoutingContext.getShard();
        KurinRoutingContext.setShard(shard);
        try {
            return joinPoint.proceed();
        } finally {
            if (previous != null) {
                KurinRoutingContext.setShard(previous);
            } else {
                KurinRoutingContext.clearShard();
            }
        }
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
