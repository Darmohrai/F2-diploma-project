package org.kurin.routing.spring;

import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.kurin.routing.consistency.ConsistencyTokenCarrier;

import java.util.Optional;

/**
 * Carries the consistency token in a header (for API clients, which echo it on their next request) and,
 * optionally, in a cookie (for browsers, which do that on their own).
 */
public class ServletConsistencyTokenCarrier implements ConsistencyTokenCarrier<HttpServletRequest, HttpServletResponse> {
    private final String headerName;
    private final String cookieName;

    /**
     * @param headerName name of the header, or empty to use no header
     * @param cookieName name of the cookie, or empty to use no cookie
     */
    public ServletConsistencyTokenCarrier(String headerName, String cookieName) {
        this.headerName = headerName == null ? "" : headerName.trim();
        this.cookieName = cookieName == null ? "" : cookieName.trim();
    }

    @Override
    public Optional<String> read(HttpServletRequest request) {
        if (!headerName.isEmpty()) {
            String header = request.getHeader(headerName);
            if (header != null && !header.isBlank()) {
                return Optional.of(header);
            }
        }
        if (!cookieName.isEmpty() && request.getCookies() != null) {
            for (Cookie cookie : request.getCookies()) {
                if (cookieName.equals(cookie.getName()) && !cookie.getValue().isBlank()) {
                    return Optional.of(cookie.getValue());
                }
            }
        }
        return Optional.empty();
    }

    @Override
    public void write(HttpServletResponse response, String encodedToken) {
        if (!headerName.isEmpty()) {
            response.setHeader(headerName, encodedToken);
        }
        if (!cookieName.isEmpty()) {
            Cookie cookie = new Cookie(cookieName, encodedToken);
            cookie.setHttpOnly(true);
            cookie.setPath("/");
            cookie.setAttribute("SameSite", "Lax");
            response.addCookie(cookie);
        }
    }
}
