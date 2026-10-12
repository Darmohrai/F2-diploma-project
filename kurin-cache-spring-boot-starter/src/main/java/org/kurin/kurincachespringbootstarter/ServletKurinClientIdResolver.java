package org.kurin.kurincachespringbootstarter;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.security.Principal;

/**
 * Uses the authenticated principal name, falling back to the id of an already existing HTTP session.
 */
public class ServletKurinClientIdResolver implements KurinClientIdResolver {

    @Override
    public String resolveClientId() {
        if (!(RequestContextHolder.getRequestAttributes() instanceof ServletRequestAttributes attributes)) {
            return null;
        }
        HttpServletRequest request = attributes.getRequest();

        Principal principal = request.getUserPrincipal();
        if (principal != null) {
            return principal.getName();
        }

        HttpSession session = request.getSession(false);
        return session != null ? session.getId() : null;
    }
}
