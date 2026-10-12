package org.kurin.kurinspringbootstarter.starter;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

@ConfigurationProperties(prefix = "kurin.serialization")
public class KurinSerializationProperties {
    /**
     * Additional classes that travel inside commands, queries or results but cannot be discovered from the
     * handler signatures, for example implementations behind an interface-typed field.
     */
    private List<Class<?>> types = new ArrayList<>();

    public List<Class<?>> getTypes() { return types; }
    public void setTypes(List<Class<?>> types) { this.types = types; }
}
