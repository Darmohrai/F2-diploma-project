package org.kurin.raft;

import org.kurin.network.NetworkStarter;
import org.kurin.network.config.NetworkConfig;
import org.kurin.network.mailbox.Mailbox;
import org.kurin.network.model.NodeAddress;
import org.kurin.raft.log.InMemoryRaftLog;
import org.kurin.raft.state.FilePersistentMetadata;
import org.kurin.raft.state.RaftState;
import org.kurin.raft.state.StateMachine;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class RaftNode {

    private final NetworkStarter networkStarter;
    private final RaftState raftState;
    private final RaftDispatcher dispatcher;
    private final ExecutorService mailboxExecutor;

    public RaftNode(NodeAddress localAddress, Set<NodeAddress> peers, StateMachine stateMachine, NetworkConfig config) {

        this.mailboxExecutor = Executors.newSingleThreadExecutor();
        Mailbox mailbox = new Mailbox(mailboxExecutor);

        String nodeId = String.valueOf(localAddress.port());

        this.raftState = new RaftState(
                localAddress,
                peers,
                new InMemoryRaftLog(),
                mailbox,
                stateMachine,
                new FilePersistentMetadata(nodeId)
        );

        this.dispatcher = new RaftDispatcher(this.raftState);

        this.networkStarter = new NetworkStarter(localAddress.port(), this.dispatcher, config);

        this.raftState.setNetworkClient(networkStarter.getClient());
        this.raftState.setTimer(networkStarter.getTimer());
    }

    public void start() {
        networkStarter.start();
        raftState.getMailbox().submit(() -> {
            raftState.transitionTo(new org.kurin.raft.state.roles.FollowerRole());
        });
    }

    public void shutdown() {
        raftState.getCurrentRole().onExit(raftState);

        networkStarter.shutdown();

        dispatcher.shutdown();

        mailboxExecutor.shutdown();
        try {
            if (!mailboxExecutor.awaitTermination(2, TimeUnit.SECONDS)) {
                mailboxExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            mailboxExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public RaftState getRaftState() {
        return raftState;
    }
}
