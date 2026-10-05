package org.kurin.raft.state;

public interface StateMachine {

    Object apply(Object command);

    byte[] takeSnapshot();

    void installSnapshot(byte[] data);

    void addListener(StateMachineListener listener);
}