package org.kurin.network.config;

public class NetworkConfig {
    /**
     * Bumped whenever the frame layout or the handshake changes incompatibly.
     */
    public static final int PROTOCOL_VERSION = 2;

    private short magicNumber = 0x5252;
    private int maxPayloadSize = 1048576;
    private int requestTimeoutMs = 5000;
    private int connectTimeoutMs = 1000;
    private int idleTimeoutSeconds = 30;
    private int workerThreads = 0;
    private String bindHost = "0.0.0.0";
    private String clusterName = "kurin";
    private String clusterSecret;
    private final TlsConfig tls = new TlsConfig();

    public short getMagicNumber() {
        return magicNumber;
    }

    public void setMagicNumber(short magicNumber) {
        this.magicNumber = magicNumber;
    }

    public int getMaxPayloadSize() {
        return maxPayloadSize;
    }

    public void setMaxPayloadSize(int maxPayloadSize) {
        if (maxPayloadSize < 1024) {
            throw new IllegalArgumentException("maxPayloadSize must be at least 1024 bytes");
        }
        this.maxPayloadSize = maxPayloadSize;
    }

    public int getRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    public void setRequestTimeoutMs(int requestTimeoutMs) {
        this.requestTimeoutMs = requestTimeoutMs;
    }

    public int getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    public void setConnectTimeoutMs(int connectTimeoutMs) {
        this.connectTimeoutMs = connectTimeoutMs;
    }

    public int getIdleTimeoutSeconds() {
        return idleTimeoutSeconds;
    }

    public void setIdleTimeoutSeconds(int idleTimeoutSeconds) {
        this.idleTimeoutSeconds = idleTimeoutSeconds;
    }

    public int getWorkerThreads() {
        return workerThreads;
    }

    public void setWorkerThreads(int workerThreads) {
        this.workerThreads = workerThreads;
    }

    public String getBindHost() {
        return bindHost;
    }

    public void setBindHost(String bindHost) {
        this.bindHost = bindHost;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        if (clusterName == null || clusterName.isBlank()) {
            throw new IllegalArgumentException("clusterName must not be blank");
        }
        this.clusterName = clusterName;
    }

    public String getClusterSecret() {
        return clusterSecret;
    }

    public void setClusterSecret(String clusterSecret) {
        this.clusterSecret = clusterSecret;
    }

    public TlsConfig getTls() {
        return tls;
    }

    public static class TlsConfig {
        private boolean enabled;
        private String keyStore;
        private String keyStorePassword;
        private String keyStoreType = "PKCS12";
        private String trustStore;
        private String trustStorePassword;
        private String trustStoreType = "PKCS12";

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getKeyStore() {
            return keyStore;
        }

        public void setKeyStore(String keyStore) {
            this.keyStore = keyStore;
        }

        public String getKeyStorePassword() {
            return keyStorePassword;
        }

        public void setKeyStorePassword(String keyStorePassword) {
            this.keyStorePassword = keyStorePassword;
        }

        public String getKeyStoreType() {
            return keyStoreType;
        }

        public void setKeyStoreType(String keyStoreType) {
            this.keyStoreType = keyStoreType;
        }

        public String getTrustStore() {
            return trustStore;
        }

        public void setTrustStore(String trustStore) {
            this.trustStore = trustStore;
        }

        public String getTrustStorePassword() {
            return trustStorePassword;
        }

        public void setTrustStorePassword(String trustStorePassword) {
            this.trustStorePassword = trustStorePassword;
        }

        public String getTrustStoreType() {
            return trustStoreType;
        }

        public void setTrustStoreType(String trustStoreType) {
            this.trustStoreType = trustStoreType;
        }
    }
}
