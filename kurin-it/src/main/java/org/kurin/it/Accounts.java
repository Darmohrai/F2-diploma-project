package org.kurin.it;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.kurin.routing.KurinPrimary;
import org.kurin.routing.KurinReadOnly;
import org.kurin.routing.KurinShardKey;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Accounts live in the shard their id hashes to. The table is created on every shard by the Liquibase change
 * log of the application.
 */
@Service
public class Accounts {
    @PersistenceContext
    private EntityManager entities;

    @Transactional
    public void save(@KurinShardKey String id, String name) {
        Account account = entities.find(Account.class, id);
        if (account == null) {
            entities.persist(new Account(id, name));
        } else {
            account.setName(name);
        }
    }

    /**
     * May be served by the replica, unless the caller wrote recently.
     */
    @KurinReadOnly
    public String nameOf(@KurinShardKey String id) {
        return read(id);
    }

    /**
     * Always served by the replica if there is one.
     */
    @KurinReadOnly(consistency = KurinReadOnly.Consistency.EVENTUAL)
    public String possiblyStaleNameOf(@KurinShardKey String id) {
        return read(id);
    }

    @KurinPrimary
    public String currentNameOf(@KurinShardKey String id) {
        return read(id);
    }

    private String read(String id) {
        Account account = entities.find(Account.class, id);
        return account != null ? account.getName() : null;
    }
}
