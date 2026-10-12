package org.kurin.kurincachespringbootstarter;

import org.kurin.kurintieredcache.api.KurinCacheException;
import org.kurin.kurintieredcache.api.KurinCacheKeyException;
import org.springframework.context.expression.MethodBasedEvaluationContext;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.ParameterNameDiscoverer;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Evaluates the SpEL expressions of the cache annotations. Each expression is parsed once.
 * <p>
 * Available in an expression: the arguments as {@code #name}, {@code #a0}, {@code #p0}; {@code #cmd} for the
 * first argument; {@code #root.methodName}, {@code #root.target}, {@code #root.args}; and {@code #result}
 * where a result exists.
 */
public class CacheExpressions {

    /**
     * Root object of every expression.
     */
    public record Root(String methodName, Object target, Object[] args) {
    }

    /**
     * Marks an evaluation that has no method result to offer.
     */
    public static final Object NO_RESULT = new Object();

    private final SpelExpressionParser parser = new SpelExpressionParser();
    private final ParameterNameDiscoverer parameterNames = new DefaultParameterNameDiscoverer();
    private final Map<String, Expression> parsed = new ConcurrentHashMap<>();

    /**
     * @throws org.springframework.expression.ParseException if the text is not a valid expression
     */
    public Expression parse(String expression) {
        return parsed.computeIfAbsent(expression, parser::parseExpression);
    }

    /**
     * @return the key the expression yields for this call
     * @throws KurinCacheKeyException if the expression fails or yields null; it is never taken as a literal key
     */
    public String key(String expression, Method method, Object target, Object[] arguments, Object result) {
        Object value;
        try {
            value = parse(expression).getValue(context(method, target, arguments, result));
        } catch (RuntimeException e) {
            throw new KurinCacheKeyException("Cannot evaluate cache key expression '" + expression + "' of "
                    + describe(method) + ": " + e.getMessage(), e);
        }
        if (value == null) {
            throw new KurinCacheKeyException("Cache key expression '" + expression + "' of " + describe(method)
                    + " evaluated to null. If it refers to a parameter by name, compile with -parameters or use #a0, #a1, ...");
        }
        return value.toString();
    }

    /**
     * @throws KurinCacheException if the expression fails or does not yield a boolean
     */
    public boolean condition(String expression, Method method, Object target, Object[] arguments, Object result) {
        try {
            Boolean value = parse(expression).getValue(context(method, target, arguments, result), Boolean.class);
            if (value == null) {
                throw new IllegalStateException("the expression evaluated to null");
            }
            return value;
        } catch (RuntimeException e) {
            throw new KurinCacheException("Cannot evaluate cache condition '" + expression + "' of " + describe(method)
                    + ": " + e.getMessage(), e);
        }
    }

    private MethodBasedEvaluationContext context(Method method, Object target, Object[] arguments, Object result) {
        MethodBasedEvaluationContext context = new MethodBasedEvaluationContext(
                new Root(method.getName(), target, arguments), method, arguments, parameterNames);
        if (arguments.length > 0) {
            context.setVariable("cmd", arguments[0]);
        }
        if (result != NO_RESULT) {
            context.setVariable("result", result);
        }
        return context;
    }

    public static String describe(Method method) {
        return method.getDeclaringClass().getSimpleName() + "." + method.getName();
    }
}
