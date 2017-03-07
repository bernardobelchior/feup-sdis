package server;

public class Peer {
    private final String protocolVersion;
    private final int serverId;

    public Peer(String protocolVersion, int serverId) {
        this.protocolVersion = protocolVersion;
        this.serverId = serverId;
    }

    public String getProtocolVersion() {
        return protocolVersion;
    }

    public int getServerId() {
        return serverId;
    }

}

