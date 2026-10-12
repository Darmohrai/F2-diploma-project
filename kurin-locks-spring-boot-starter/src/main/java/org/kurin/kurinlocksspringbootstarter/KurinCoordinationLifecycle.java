package org.kurin.kurinlocksspringbootstarter;

import org.kurin.api.LeadershipToken;
import org.kurin.kurinlocks.command.LockCommands.PurgeExpiredLocks;
import org.kurin.kurinlocks.leader.CancellationSignal;
import org.kurin.kurinlocks.leader.KurinLeaderTasks;
import org.kurin.kurinlocks.leader.LeaderTask;
import org.kurin.raft.KurinNode;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Starts the leader tasks after the node is running and stops them before the node shuts down.
 */
public class KurinCoordinationLifecycle implements SmartLifecycle {
    /**
     * Right after the node lifecycle (phase 0) on start, right before it on stop.
     */
    public static final int PHASE = 100;
    public static final String PURGE_TASK = "kurin.locks.purge";

    private final KurinNode node;
    private final KurinLeaderTasks leaderTasks;
    private final KurinCoordinationRegistry registry;
    private final BeanFactory beanFactory;
    private final KurinLocksProperties properties;
    private volatile boolean running;

    public KurinCoordinationLifecycle(KurinNode node, KurinLeaderTasks leaderTasks, KurinCoordinationRegistry registry,
                                      BeanFactory beanFactory, KurinLocksProperties properties) {
        this.node = node;
        this.leaderTasks = leaderTasks;
        this.registry = registry;
        this.beanFactory = beanFactory;
        this.properties = properties;
    }

    @Override
    public void start() {
        // Expired locks are overwritten on demand; the purge only keeps the table from growing with unused keys
        Duration timeout = properties.getCommandTimeout();
        leaderTasks.register(PURGE_TASK, properties.getPurgeInterval(), (token, signal) ->
                node.submitCommand(new PurgeExpiredLocks(), timeout).get(timeout.toMillis() + 1000, TimeUnit.MILLISECONDS));

        for (KurinCoordinationRegistry.LeaderTaskMethod task : registry.leaderTasks()) {
            leaderTasks.register(task.name(), task.interval(), invoker(task));
        }
        leaderTasks.start();
        running = true;
    }

    // The task is called through the final bean, so its other advice (transactions, locks) applies
    private LeaderTask invoker(KurinCoordinationRegistry.LeaderTaskMethod task) {
        Object bean = beanFactory.getBean(task.beanName());
        Method method = AopUtils.selectInvocableMethod(task.method(), bean.getClass());
        ReflectionUtils.makeAccessible(method);
        int parameters = method.getParameterCount();
        return (LeadershipToken token, CancellationSignal signal) -> {
            try {
                if (parameters == 0) {
                    method.invoke(bean);
                } else if (parameters == 1) {
                    method.invoke(bean, token);
                } else {
                    method.invoke(bean, token, signal);
                }
            } catch (InvocationTargetException e) {
                if (e.getCause() instanceof Exception cause) {
                    throw cause;
                }
                throw e;
            }
        };
    }

    @Override
    public void stop() {
        running = false;
        leaderTasks.close();
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return PHASE;
    }
}
