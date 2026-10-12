package org.kurin.kurinlocks.leader;

import org.kurin.api.LeadershipToken;

/**
 * Work that should run on the leader only.
 */
@FunctionalInterface
public interface LeaderTask {

    /**
     * @param token  the leadership under which the run was started; its term is a fencing token
     * @param signal becomes cancelled when this node loses leadership during the run
     */
    void run(LeadershipToken token, CancellationSignal signal) throws Exception;
}
