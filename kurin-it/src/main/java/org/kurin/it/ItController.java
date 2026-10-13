package org.kurin.it;

import org.kurin.api.ReadConsistency;
import org.kurin.kurinspringbootstarter.template.KurinTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.TimeUnit;

/**
 * Plain text in and out, so that the tests need no JSON library, which differs between Spring Boot 3 and 4.
 */
@RestController
public class ItController {
    private final KurinTemplate kurin;
    private final Accounts accounts;
    private final Profiles profiles;

    public ItController(KurinTemplate kurin, Accounts accounts, Profiles profiles) {
        this.kurin = kurin;
        this.accounts = accounts;
        this.profiles = profiles;
    }

    @PostMapping("/counter")
    public String add(@RequestParam int delta) throws Exception {
        return String.valueOf(kurin.<Integer>submitCommand(new Counters.Add(delta)).get(10, TimeUnit.SECONDS));
    }

    @GetMapping("/counter")
    public String counter() throws Exception {
        return String.valueOf(kurin.<Integer>query(new Counters.Read(), ReadConsistency.LINEARIZABLE).get(10, TimeUnit.SECONDS));
    }

    @PostMapping("/accounts/{id}")
    public String save(@PathVariable String id, @RequestParam String name) {
        accounts.save(id, name);
        return "saved";
    }

    @GetMapping("/accounts/{id}")
    public String name(@PathVariable String id) {
        return String.valueOf(accounts.nameOf(id));
    }

    @GetMapping("/accounts/{id}/stale")
    public String possiblyStaleName(@PathVariable String id) {
        return String.valueOf(accounts.possiblyStaleNameOf(id));
    }

    @PostMapping("/profiles/{id}")
    public String rename(@PathVariable String id, @RequestParam String name) {
        profiles.rename(id, name);
        return "renamed";
    }

    @GetMapping("/profiles/{id}")
    public String profile(@PathVariable String id) {
        return profiles.profile(id);
    }
}
