package org.kurin.kurincachespringbootstarter;

import org.kurin.kurintieredcache.api.KurinCacheEvict;
import org.kurin.kurintieredcache.api.KurinCachePut;
import org.kurin.kurintieredcache.api.KurinCacheable;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.convert.DurationStyle;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.expression.ParseException;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;

/**
 * Checks the cache annotations of every bean when the bean is created, so that a mistake stops the application
 * at startup instead of surfacing on some later call.
 */
public class KurinCacheAnnotationValidator implements BeanPostProcessor {
    private final SpelExpressionParser parser = new SpelExpressionParser();

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
        Class<?> type = AopUtils.getTargetClass(bean);
        if (type.getName().startsWith("org.springframework.") || type.getName().startsWith("java.")) {
            return bean;
        }
        try {
            ReflectionUtils.doWithMethods(type, this::validate, method -> !method.isBridge() && !method.isSynthetic());
        } catch (LinkageError e) {
            // The class refers to optional dependencies that are absent, so it cannot carry our annotations either
            return bean;
        }
        return bean;
    }

    private void validate(Method method) {
        String where = method.getDeclaringClass().getName() + "." + method.getName();

        KurinCacheable cacheable = AnnotatedElementUtils.findMergedAnnotation(method, KurinCacheable.class);
        if (cacheable != null) {
            String annotation = "@KurinCacheable on " + where;
            requireName(cacheable.cacheName(), annotation);
            if (method.getReturnType() == void.class) {
                throw new IllegalStateException(annotation + ": a void method has no result to cache");
            }
            if (cacheable.cacheNulls() && method.getReturnType().isPrimitive()) {
                throw new IllegalStateException(annotation + ": cacheNulls cannot be used with the primitive return type '"
                        + method.getReturnType().getName() + "'");
            }
            if (cacheable.jitterPercent() > 100) {
                throw new IllegalStateException(annotation + ": jitterPercent must be between 0 and 100");
            }
            expression(cacheable.key(), "key", annotation);
            expression(cacheable.condition(), "condition", annotation);
            expression(cacheable.unless(), "unless", annotation);
            duration(cacheable.ttl(), "ttl", annotation);
            duration(cacheable.nullTtl(), "nullTtl", annotation);
            duration(cacheable.staleWhileRevalidate(), "staleWhileRevalidate", annotation);
            duration(cacheable.lockWait(), "lockWait", annotation);
        }

        KurinCachePut put = AnnotatedElementUtils.findMergedAnnotation(method, KurinCachePut.class);
        if (put != null) {
            String annotation = "@KurinCachePut on " + where;
            requireName(put.cacheName(), annotation);
            if (method.getReturnType() == void.class) {
                throw new IllegalStateException(annotation + ": a void method has no result to store");
            }
            requireKey(put.key(), method, put, null, annotation);
            expression(put.key(), "key", annotation);
            duration(put.ttl(), "ttl", annotation);
        }

        KurinCacheEvict evict = AnnotatedElementUtils.findMergedAnnotation(method, KurinCacheEvict.class);
        if (evict != null) {
            String annotation = "@KurinCacheEvict on " + where;
            if (evict.cacheNames().length == 0) {
                throw new IllegalStateException(annotation + " needs at least one cache name");
            }
            for (String cacheName : evict.cacheNames()) {
                requireName(cacheName, annotation);
            }
            requireKey(evict.key(), method, null, evict, annotation);
            expression(evict.key(), "key", annotation);
        }
    }

    private static void requireName(String cacheName, String annotation) {
        if (cacheName.isBlank()) {
            throw new IllegalStateException(annotation + " needs a cache name");
        }
    }

    private static void requireKey(String key, Method method, KurinCachePut put, KurinCacheEvict evict, String annotation) {
        if (key.isEmpty() && !CacheAnnotationSupport.keyIsOptional(method, put, evict)) {
            throw new IllegalStateException(annotation + " needs a key expression"
                    + (evict != null ? ", allEntries = true," : "") + " or a first parameter that implements CacheKeyProvider");
        }
    }

    private void expression(String expression, String attribute, String annotation) {
        if (expression.isEmpty()) {
            return;
        }
        try {
            parser.parseExpression(expression);
        } catch (ParseException e) {
            throw new IllegalStateException(annotation + " has an invalid " + attribute + " expression '" + expression
                    + "': " + e.getMessage() + ". A constant must be quoted, for example \"'all'\".", e);
        }
    }

    private static void duration(String value, String attribute, String annotation) {
        if (value.isEmpty()) {
            return;
        }
        try {
            if (DurationStyle.detectAndParse(value).isNegative()) {
                throw new IllegalArgumentException("negative duration");
            }
        } catch (RuntimeException e) {
            throw new IllegalStateException(annotation + " has an invalid " + attribute + " '" + value
                    + "'. Use an ISO-8601 duration such as PT5M or a simple one such as 5m.", e);
        }
    }
}
