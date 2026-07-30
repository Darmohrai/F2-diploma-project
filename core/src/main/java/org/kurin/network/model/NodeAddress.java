package org.kurin.network.model;

public record NodeAddress(String host, int port) {
    public String asString() {
        return host + ":" + port;
    }
}
