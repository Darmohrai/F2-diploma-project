package org.kurin.network.model;

public class MessageType {
    public static final byte REQUEST = 1;
    public static final byte RESPONSE = 2;
    public static final byte HELLO = 4;
    public static final byte HELLO_ACK = 5;
    public static final byte HELLO_REJECT = 6;

    private MessageType() {
    }

    public static boolean isHandshake(byte type) {
        return type == HELLO || type == HELLO_ACK || type == HELLO_REJECT;
    }
}
