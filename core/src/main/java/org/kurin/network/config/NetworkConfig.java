package org.kurin.network.config;

public class NetworkConfig {
    private short magicNumber = 0x5252;
    private int maxPayloadSize = 1048576;
    private int requestTimeoutSeconds = 5;
    private int workerThreads = 0;

    public NetworkConfig() {
    }

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
        this.maxPayloadSize = maxPayloadSize;
    }

    public int getRequestTimeoutSeconds() {
        return requestTimeoutSeconds;
    }

    public void setRequestTimeoutSeconds(int requestTimeoutSeconds) {
        this.requestTimeoutSeconds = requestTimeoutSeconds;
    }

    public int getWorkerThreads() {
        return workerThreads;
    }

    public void setWorkerThreads(int workerThreads) {
        this.workerThreads = workerThreads;
    }
}
