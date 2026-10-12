package org.kurin.network.model;

/**
 * Advertised address of a node. Its string form {@code host:port} is the node id.
 */
public record NodeAddress(String host, int port) {

    public String asString() {
        return host + ":" + port;
    }

    public static NodeAddress parse(String value) {
        int separator = value.lastIndexOf(':');
        if (separator <= 0 || separator == value.length() - 1) {
            throw new IllegalArgumentException("Node address must look like host:port, got '" + value + "'");
        }
        try {
            return new NodeAddress(value.substring(0, separator).trim(), Integer.parseInt(value.substring(separator + 1).trim()));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Node address must look like host:port, got '" + value + "'");
        }
    }

    @Override
    public String toString() {
        return asString();
    }
}
