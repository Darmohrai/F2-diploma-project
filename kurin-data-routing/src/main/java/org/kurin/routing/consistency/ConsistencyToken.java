package org.kurin.routing.consistency;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * What a client has to carry between requests so that any application instance can give it read-your-writes:
 * per shard, a value that says how recent the client's last write was. The meaning of the value belongs to the
 * {@link ConsistencyTokenStrategy} in use.
 * <p>
 * The wire form is {@code base64url(shard=value;...)} followed by {@code .} and an HMAC-SHA256 of it when a
 * secret is configured. The token holds no user data and nothing secret; the signature only keeps clients from
 * forging positions.
 */
public final class ConsistencyToken {
    private static final ConsistencyToken EMPTY = new ConsistencyToken(Map.of());

    private final Map<String, String> positions;

    private ConsistencyToken(Map<String, String> positions) {
        this.positions = Map.copyOf(positions);
    }

    public static ConsistencyToken empty() {
        return EMPTY;
    }

    public boolean isEmpty() {
        return positions.isEmpty();
    }

    /**
     * @return the value recorded for the shard, or null if the client has not written to it
     */
    public String position(String shard) {
        return positions.get(shard);
    }

    public Map<String, String> positions() {
        return positions;
    }

    /**
     * @return a token that also records the given write
     */
    public ConsistencyToken with(String shard, String position) {
        Map<String, String> updated = new TreeMap<>(positions);
        updated.put(shard, position);
        return new ConsistencyToken(updated);
    }

    /**
     * @param secret signing key, or null to leave the token unsigned
     */
    public String encode(String secret) {
        StringBuilder plain = new StringBuilder();
        new TreeMap<>(positions).forEach((shard, position) -> {
            if (plain.length() > 0) {
                plain.append(';');
            }
            plain.append(escape(shard)).append('=').append(escape(position));
        });
        String payload = Base64.getUrlEncoder().withoutPadding().encodeToString(plain.toString().getBytes(StandardCharsets.UTF_8));
        return hasSecret(secret) ? payload + "." + sign(payload, secret) : payload;
    }

    /**
     * @return the token, or empty if the text is malformed or its signature does not match
     */
    public static Optional<ConsistencyToken> decode(String encoded, String secret) {
        if (encoded == null || encoded.isBlank()) {
            return Optional.empty();
        }
        String payload = encoded.trim();
        if (hasSecret(secret)) {
            int separator = payload.lastIndexOf('.');
            if (separator < 0) {
                return Optional.empty();
            }
            String signature = payload.substring(separator + 1);
            payload = payload.substring(0, separator);
            if (!MessageDigest.isEqual(signature.getBytes(StandardCharsets.UTF_8), sign(payload, secret).getBytes(StandardCharsets.UTF_8))) {
                return Optional.empty();
            }
        } else if (payload.indexOf('.') >= 0) {
            // Signed by someone, but this instance has no key to check it with
            payload = payload.substring(0, payload.lastIndexOf('.'));
        }
        try {
            String plain = new String(Base64.getUrlDecoder().decode(payload), StandardCharsets.UTF_8);
            Map<String, String> positions = new TreeMap<>();
            if (!plain.isEmpty()) {
                for (String entry : plain.split(";")) {
                    int equals = entry.indexOf('=');
                    if (equals <= 0) {
                        return Optional.empty();
                    }
                    positions.put(unescape(entry.substring(0, equals)), unescape(entry.substring(equals + 1)));
                }
            }
            return Optional.of(new ConsistencyToken(positions));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    private static boolean hasSecret(String secret) {
        return secret != null && !secret.isEmpty();
    }

    private static String sign(String payload, String secret) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            return HexFormat.of().formatHex(mac.doFinal(payload.getBytes(StandardCharsets.UTF_8)));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HmacSHA256 is not available", e);
        }
    }

    // Shard names and positions must not break the "a=b;c=d" form
    private static String escape(String value) {
        return value.replace("%", "%25").replace(";", "%3B").replace("=", "%3D");
    }

    private static String unescape(String value) {
        return value.replace("%3D", "=").replace("%3B", ";").replace("%25", "%");
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ConsistencyToken token && positions.equals(token.positions);
    }

    @Override
    public int hashCode() {
        return positions.hashCode();
    }

    @Override
    public String toString() {
        return "ConsistencyToken" + positions;
    }
}
