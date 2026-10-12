package org.kurin.kurincachespringbootstarter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.kurin.kurintieredcache.api.KurinCacheTrace;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.util.ContentCachingResponseWrapper;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Reports the cache lookups of a request in the {@code X-Kurin-Cache} response header, as
 * {@code <cache>=<HIT_L1|HIT_L2|HIT_L3|STALE|MISS>;...} in the order they happened. A diagnostic aid: it shows
 * from which tier a response was served. Only lookups made on the request thread are seen.
 * <p>
 * The response body is buffered until the request is finished, because the header can only be written before
 * the body.
 */
public class KurinCacheTraceFilter extends OncePerRequestFilter {
    public static final String HEADER = "X-Kurin-Cache";

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        ContentCachingResponseWrapper buffered = new ContentCachingResponseWrapper(response);
        KurinCacheTrace.begin();
        try {
            chain.doFilter(request, buffered);
        } finally {
            List<KurinCacheTrace.Lookup> lookups = KurinCacheTrace.end();
            if (!lookups.isEmpty() && !response.isCommitted()) {
                response.setHeader(HEADER, lookups.stream()
                        .map(lookup -> lookup.cache() + "=" + lookup.result())
                        .collect(Collectors.joining(";")));
            }
            buffered.copyBodyToResponse();
        }
    }
}
