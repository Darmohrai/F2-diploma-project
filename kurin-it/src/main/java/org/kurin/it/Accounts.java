package org.kurin.it;

import org.kurin.routing.KurinPrimary;
import org.kurin.routing.KurinReadOnly;
import org.kurin.routing.KurinShardKey;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Accounts live in the shard their id hashes to.
 */
@Service
public class Accounts {
    private final JdbcTemplate jdbc;

    public Accounts(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Transactional
    public void save(@KurinShardKey String id, String name) {
        if (jdbc.update("update accounts set name = ? where id = ?", name, id) == 0) {
            jdbc.update("insert into accounts (id, name) values (?, ?)", id, name);
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
        List<String> names = jdbc.queryForList("select name from accounts where id = ?", String.class, id);
        return names.isEmpty() ? null : names.get(0);
    }
}
