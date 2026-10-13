package org.kurin.routing.health;

import javax.sql.DataSource;
import java.lang.reflect.Method;
import java.util.Optional;

/**
 * What it takes to open a connection to a database node without going through a pool.
 *
 * @param username may be null if the URL carries the credentials or the database asks for none
 * @param password may be null
 */
public record ConnectionParameters(String url, String username, String password) {

    public ConnectionParameters {
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException("A JDBC URL is required");
        }
    }

    /**
     * Reads the parameters from a data source that exposes them the way connection pools do: a getter
     * {@code getJdbcUrl()} or {@code getUrl()}, and {@code getUsername()}, {@code getUserName()} or
     * {@code getUser()} with {@code getPassword()}. HikariCP, Commons DBCP and the simple data sources of Spring
     * do. Tomcat JDBC keeps them in the object behind {@code getPoolProperties()}, which is read when present.
     *
     * @return empty if the data source names no URL, for example because it wraps another data source or
     * is configured with a driver-specific {@code DataSource} class instead of a URL
     */
    public static Optional<ConnectionParameters> of(DataSource dataSource) {
        Object settings = settingsOf(dataSource);
        String url = read(settings, "getJdbcUrl", "getUrl", "getURL");
        if (url == null || url.isBlank()) {
            return Optional.empty();
        }
        return Optional.of(new ConnectionParameters(url, read(settings, "getUsername", "getUserName", "getUser"),
                read(settings, "getPassword")));
    }

    // Tomcat JDBC answers getPassword() of the data source itself with a notice that it is not available
    private static Object settingsOf(DataSource dataSource) {
        try {
            Object properties = dataSource.getClass().getMethod("getPoolProperties").invoke(dataSource);
            return properties != null ? properties : dataSource;
        } catch (ReflectiveOperationException | RuntimeException e) {
            // No such thing: the data source speaks for itself
            return dataSource;
        }
    }

    // The first of the getters that exists and returns a string
    private static String read(Object dataSource, String... getters) {
        for (String getter : getters) {
            try {
                Method method = dataSource.getClass().getMethod(getter);
                if (method.getReturnType() == String.class) {
                    Object value = method.invoke(dataSource);
                    if (value != null) {
                        return (String) value;
                    }
                }
            } catch (NoSuchMethodException e) {
                // This kind of data source calls it differently: try the next name
                continue;
            } catch (ReflectiveOperationException | RuntimeException e) {
                // The getter exists and cannot be used (not accessible, or it refuses): as good as absent
                return null;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return "ConnectionParameters[url=" + url + ", username=" + username + "]";
    }
}
