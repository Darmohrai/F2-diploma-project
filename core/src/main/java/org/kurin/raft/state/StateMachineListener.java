package org.kurin.raft.state;

public interface StateMachineListener {

    void onCommandApplied(Object command, Object result);
}
