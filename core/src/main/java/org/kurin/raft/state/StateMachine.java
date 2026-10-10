package org.kurin.raft.state;

public interface StateMachine {

    Object apply(Object command);

    Object executeQuery(Object query);

    byte[] takeSnapshot();

    void installSnapshot(byte[] data);

    void addListener(StateMachineListener listener);
}