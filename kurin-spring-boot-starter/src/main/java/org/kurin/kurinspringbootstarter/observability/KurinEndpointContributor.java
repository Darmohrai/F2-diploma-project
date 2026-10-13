package org.kurin.kurinspringbootstarter.observability;

/**
 * Adds a section to the {@code /actuator/kurin} endpoint. The other Kurin starters describe their part of the
 * system through beans of this type; an application may add its own.
 */
public interface KurinEndpointContributor {

    /**
     * @return key of the section in the endpoint response
     */
    String name();

    /**
     * @return the content of the section: maps, lists and simple values that serialize to JSON. Called on
     * every read of the endpoint, so it must be cheap and must not block for long.
     */
    Object describe();
}
