package org.kurin.test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;

/**
 * Finds free loopback ports for tests, so that no test depends on a fixed port number.
 */
public final class TestPorts {
    private TestPorts() {
    }

    public static int freePort() {
        return freePorts(1).get(0);
    }

    /**
     * @return distinct ports that were free at the time of the call
     */
    public static List<Integer> freePorts(int count) {
        List<ServerSocket> sockets = new ArrayList<>();
        try {
            List<Integer> ports = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                // All sockets stay open until every port is chosen, so the same port is never handed out twice
                ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
                sockets.add(socket);
                ports.add(socket.getLocalPort());
            }
            return ports;
        } catch (IOException e) {
            throw new IllegalStateException("Cannot find free ports", e);
        } finally {
            for (ServerSocket socket : sockets) {
                try {
                    socket.close();
                } catch (IOException e) {
                    throw new IllegalStateException("Cannot release a probed port", e);
                }
            }
        }
    }
}
