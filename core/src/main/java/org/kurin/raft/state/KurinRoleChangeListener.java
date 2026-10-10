package org.kurin.raft.state;

public interface KurinRoleChangeListener {

    void onLeaderElected();

    void onLeaderLost();
}
