package org.kurin.raft.state;

import org.kurin.raft.RaftStatus;

/**
 * Notified when this node gains or loses leadership. Runs on the Raft thread: implementations must
 * hand off any real work to another thread.
 */
public interface RoleChangeListener {
    void onLeaderElected(RaftStatus status);

    void onLeaderLost(RaftStatus status);
}
