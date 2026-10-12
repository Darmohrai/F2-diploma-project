package org.kurin.network.tls;

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.kurin.network.config.NetworkConfig;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;

/**
 * Builds mutual-TLS contexts for node-to-node connections from the configured key and trust stores.
 */
public final class SslContexts {
    private final SslContext server;
    private final SslContext client;

    private SslContexts(SslContext server, SslContext client) {
        this.server = server;
        this.client = client;
    }

    public static SslContexts create(NetworkConfig.TlsConfig tls) {
        if (tls.getKeyStore() == null || tls.getTrustStore() == null) {
            throw new IllegalArgumentException("TLS is enabled: both keyStore and trustStore must be configured");
        }
        try {
            char[] keyPassword = password(tls.getKeyStorePassword());
            KeyManagerFactory keyManagers = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagers.init(load(tls.getKeyStore(), tls.getKeyStoreType(), keyPassword), keyPassword);

            TrustManagerFactory trustManagers = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagers.init(load(tls.getTrustStore(), tls.getTrustStoreType(), password(tls.getTrustStorePassword())));

            SslContext server = SslContextBuilder.forServer(keyManagers)
                    .trustManager(trustManagers)
                    .clientAuth(ClientAuth.REQUIRE)
                    .build();
            SslContext client = SslContextBuilder.forClient()
                    .keyManager(keyManagers)
                    .trustManager(trustManagers)
                    .build();
            return new SslContexts(server, client);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot initialize TLS for Kurin node connections: " + e.getMessage(), e);
        }
    }

    public SslContext server() {
        return server;
    }

    public SslContext client() {
        return client;
    }

    private static KeyStore load(String location, String type, char[] password) throws Exception {
        KeyStore store = KeyStore.getInstance(type);
        try (InputStream in = Files.newInputStream(Path.of(location))) {
            store.load(in, password);
        }
        return store;
    }

    private static char[] password(String value) {
        return value != null ? value.toCharArray() : new char[0];
    }
}
