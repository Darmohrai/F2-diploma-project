package org.kurin.kurintieredcache.key;

import org.kurin.kurintieredcache.api.KurinCacheKeyException;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.RecordComponent;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Renders argument values into key text such that different values always give different text.
 * <p>
 * Strings are quoted and escaped, so a string can never look like a number, like {@code null} or like two
 * arguments. Records and collections are rendered from their parts; sets and maps in sorted order, so that
 * equal ones render equally. A type whose text form is not known to be unambiguous is refused.
 */
public final class CacheKeyRenderer {
    private CacheKeyRenderer() {
    }

    public static String render(Object value) {
        StringBuilder text = new StringBuilder();
        append(text, value);
        return text.toString();
    }

    public static void append(StringBuilder out, Object value) {
        if (value == null) {
            out.append("null");
        } else if (value instanceof CharSequence text) {
            appendQuoted(out, text);
        } else if (value instanceof Character character) {
            out.append('\'');
            if (character == '\'' || character == '\\') {
                out.append('\\');
            }
            out.append(character.charValue()).append('\'');
        } else if (value instanceof Number || value instanceof Boolean || value instanceof UUID
                || value instanceof TemporalAccessor || value instanceof TemporalAmount) {
            out.append(value);
        } else if (value instanceof Enum<?> constant) {
            out.append(constant.getDeclaringClass().getSimpleName()).append('.').append(constant.name());
        } else if (value instanceof Optional<?> optional) {
            out.append("Optional[");
            if (optional.isPresent()) {
                append(out, optional.get());
            }
            out.append(']');
        } else if (value instanceof Set<?> set) {
            appendSorted(out, set, '{', '}');
        } else if (value instanceof Collection<?> collection) {
            out.append('[');
            boolean first = true;
            for (Object element : collection) {
                if (!first) {
                    out.append(',');
                }
                first = false;
                append(out, element);
            }
            out.append(']');
        } else if (value instanceof Map<?, ?> map) {
            List<String> entries = new ArrayList<>(map.size());
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                entries.add(render(entry.getKey()) + "=" + render(entry.getValue()));
            }
            Collections.sort(entries);
            out.append('{').append(String.join(",", entries)).append('}');
        } else if (value.getClass().isArray()) {
            out.append('[');
            int length = Array.getLength(value);
            for (int i = 0; i < length; i++) {
                if (i > 0) {
                    out.append(',');
                }
                append(out, Array.get(value, i));
            }
            out.append(']');
        } else if (value.getClass().isRecord()) {
            appendRecord(out, value);
        } else {
            throw new KurinCacheKeyException("Cannot derive a cache key from an argument of type " + value.getClass().getName()
                    + ": its text form is not known to be unique. Give the annotation an explicit key expression, "
                    + "for example key = \"#a0.id\".");
        }
    }

    private static void appendQuoted(StringBuilder out, CharSequence text) {
        out.append('"');
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '"' || c == '\\') {
                out.append('\\');
            }
            out.append(c);
        }
        out.append('"');
    }

    private static void appendSorted(StringBuilder out, Collection<?> elements, char open, char close) {
        List<String> rendered = new ArrayList<>(elements.size());
        for (Object element : elements) {
            rendered.add(render(element));
        }
        Collections.sort(rendered);
        out.append(open).append(String.join(",", rendered)).append(close);
    }

    private static void appendRecord(StringBuilder out, Object record) {
        out.append(record.getClass().getSimpleName()).append('(');
        RecordComponent[] components = record.getClass().getRecordComponents();
        for (int i = 0; i < components.length; i++) {
            if (i > 0) {
                out.append(',');
            }
            try {
                components[i].getAccessor().setAccessible(true);
                append(out, components[i].getAccessor().invoke(record));
            } catch (IllegalAccessException | InvocationTargetException | RuntimeException e) {
                if (e instanceof KurinCacheKeyException keyException) {
                    throw keyException;
                }
                throw new KurinCacheKeyException("Cannot read component '" + components[i].getName() + "' of "
                        + record.getClass().getName() + " for the cache key", e);
            }
        }
        out.append(')');
    }
}
