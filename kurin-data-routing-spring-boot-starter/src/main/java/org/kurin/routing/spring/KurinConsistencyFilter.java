package org.kurin.routing.spring;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpServletResponseWrapper;
import org.kurin.routing.consistency.ConsistencyToken;
import org.kurin.routing.consistency.ConsistencyTokenCarrier;
import org.kurin.routing.consistency.KurinConsistency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Optional;

/**
 * Gives every request its consistency token: reads the one the client sent, makes it current for the request
 * thread, and sends the updated one back if the request wrote something. The server keeps no state; whichever
 * instance gets the client's next request learns from the token that the client has written.
 * <p>
 * A token that fails its signature check is ignored, with a warning: the request is served as if the client
 * had sent none.
 */
public class KurinConsistencyFilter extends OncePerRequestFilter {
    private static final Logger log = LoggerFactory.getLogger(KurinConsistencyFilter.class);

    private final ConsistencyTokenCarrier<HttpServletRequest, HttpServletResponse> carrier;
    private final String secret;

    public KurinConsistencyFilter(ConsistencyTokenCarrier<HttpServletRequest, HttpServletResponse> carrier, String secret) {
        this.carrier = carrier;
        this.secret = secret;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        ConsistencyToken incoming = ConsistencyToken.empty();
        Optional<String> encoded = carrier.read(request);
        if (encoded.isPresent()) {
            Optional<ConsistencyToken> decoded = ConsistencyToken.decode(encoded.get(), secret);
            if (decoded.isPresent()) {
                incoming = decoded.get();
            } else {
                log.warn("Ignoring a consistency token with an invalid signature or format from {}", request.getRemoteAddr());
            }
        }

        ConsistencyToken received = incoming;
        ConsistencyToken previous = KurinConsistency.bind(incoming);
        // Headers can be set only until the response is committed, so the token is written at the last moment before that
        Runnable writeToken = new Runnable() {
            private boolean done;

            @Override
            public void run() {
                if (done) {
                    return;
                }
                done = true;
                ConsistencyToken current = KurinConsistency.current();
                if (!current.isEmpty() && !current.equals(received)) {
                    carrier.write(response, current.encode(secret));
                }
            }
        };
        try {
            chain.doFilter(request, new BeforeCommitResponse(response, writeToken));
            if (!response.isCommitted()) {
                writeToken.run();
            }
        } finally {
            KurinConsistency.restore(previous);
        }
    }

    /**
     * Runs a callback right before the first thing that can commit the response.
     */
    private static final class BeforeCommitResponse extends HttpServletResponseWrapper {
        private final Runnable beforeCommit;

        BeforeCommitResponse(HttpServletResponse response, Runnable beforeCommit) {
            super(response);
            this.beforeCommit = beforeCommit;
        }

        @Override
        public ServletOutputStream getOutputStream() throws IOException {
            beforeCommit.run();
            return super.getOutputStream();
        }

        @Override
        public PrintWriter getWriter() throws IOException {
            beforeCommit.run();
            return super.getWriter();
        }

        @Override
        public void flushBuffer() throws IOException {
            beforeCommit.run();
            super.flushBuffer();
        }

        @Override
        public void sendError(int status) throws IOException {
            beforeCommit.run();
            super.sendError(status);
        }

        @Override
        public void sendError(int status, String message) throws IOException {
            beforeCommit.run();
            super.sendError(status, message);
        }

        @Override
        public void sendRedirect(String location) throws IOException {
            beforeCommit.run();
            super.sendRedirect(location);
        }
    }
}
